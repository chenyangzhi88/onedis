fn search_entries(db: &Db, search: &Geosearch) -> Result<Vec<GeoResult>, Error> {
    let center = match &search.center {
        GeoCenter::Coord(lon, lat) => (*lon, *lat),
        GeoCenter::Member(member) => db
            .zset_score(&search.key, member)?
            .map(|score| decode_score(score as u64))
            .ok_or_else(|| Error::msg("ERR could not decode requested zset member"))?,
    };
    if search.options.count_any {
        let limit = search.options.count.unwrap_or(usize::MAX);
        let mut entries = db
            .zset_filter_entries_limited(&search.key, limit, |_, raw_score| {
                let score = raw_score as u64;
                let point = decode_score(score);
                let distance = distance_m(center, point);
                shape_contains(&search.shape, center, point, distance)
            })?
            .into_iter()
            .filter_map(|(member, raw_score)| {
                geo_result_for_entry(center, &search.shape, member, raw_score)
            })
            .collect::<Vec<_>>();
        finalize_results(
            &mut entries,
            effective_sort(&search.options),
            search.options.count,
        );
        return Ok(entries);
    }
    let mut entries = Vec::new();
    let sort = effective_sort(&search.options);
    let response_limit = search
        .store
        .is_none()
        .then(|| max_geo_result_count(&search.options));
    let mut response_exceeded = false;
    db.zset_visit_entries(&search.key, |member, raw_score| {
        if let Some(entry) = geo_result_for_entry(center, &search.shape, member, raw_score) {
            entries.push(entry);
            compact_counted_results(&mut entries, sort, search.options.count);
            if response_limit.is_some_and(|limit| entries.len() > limit) {
                response_exceeded = true;
                return false;
            }
        }
        true
    })?;
    if response_exceeded {
        return Err(Error::msg("ERR response exceeds configured limit"));
    }
    finalize_results(&mut entries, sort, search.options.count);
    Ok(entries)
}

async fn search_entries_async(db: &Db, search: &Geosearch) -> Result<Vec<GeoResult>, Error> {
    let center = match &search.center {
        GeoCenter::Coord(lon, lat) => (*lon, *lat),
        GeoCenter::Member(member) => db
            .zset_score_async(&search.key, member)
            .await?
            .map(|score| decode_score(score as u64))
            .ok_or_else(|| Error::msg("ERR could not decode requested zset member"))?,
    };
    if search.options.count_any {
        let limit = search.options.count.unwrap_or(usize::MAX);
        let mut entries = db
            .zset_filter_entries_limited_async(&search.key, limit, |_, raw_score| {
                let score = raw_score as u64;
                let point = decode_score(score);
                let distance = distance_m(center, point);
                shape_contains(&search.shape, center, point, distance)
            })
            .await?
            .into_iter()
            .filter_map(|(member, raw_score)| {
                geo_result_for_entry(center, &search.shape, member, raw_score)
            })
            .collect::<Vec<_>>();
        finalize_results(
            &mut entries,
            effective_sort(&search.options),
            search.options.count,
        );
        return Ok(entries);
    }
    let mut entries = Vec::new();
    let sort = effective_sort(&search.options);
    let response_limit = search
        .store
        .is_none()
        .then(|| max_geo_result_count(&search.options));
    let mut response_exceeded = false;
    db.zset_visit_entries_async(&search.key, |member, raw_score| {
        if let Some(entry) = geo_result_for_entry(center, &search.shape, member, raw_score) {
            entries.push(entry);
            compact_counted_results(&mut entries, sort, search.options.count);
            if response_limit.is_some_and(|limit| entries.len() > limit) {
                response_exceeded = true;
                return false;
            }
        }
        true
    })
    .await?;
    if response_exceeded {
        return Err(Error::msg("ERR response exceeds configured limit"));
    }
    finalize_results(&mut entries, sort, search.options.count);
    Ok(entries)
}

fn effective_sort(options: &SearchOptions) -> Option<GeoSort> {
    options.sort.or_else(|| {
        options
            .count
            .filter(|_| !options.count_any)
            .map(|_| GeoSort::Asc)
    })
}

fn sort_results(entries: &mut [GeoResult], sort: GeoSort) {
    entries.sort_by(|a, b| {
        let ord = a
            .distance_m
            .total_cmp(&b.distance_m)
            .then_with(|| a.member.cmp(&b.member));
        match sort {
            GeoSort::Asc => ord,
            GeoSort::Desc => ord.reverse(),
        }
    });
}

fn compact_counted_results(
    entries: &mut Vec<GeoResult>,
    sort: Option<GeoSort>,
    count: Option<usize>,
) {
    let (Some(sort), Some(count)) = (sort, count) else {
        return;
    };
    let compact_at = count.saturating_mul(2).max(count.saturating_add(1));
    if entries.len() >= compact_at {
        sort_results(entries, sort);
        entries.truncate(count);
    }
}

fn finalize_results(entries: &mut Vec<GeoResult>, sort: Option<GeoSort>, count: Option<usize>) {
    if let Some(sort) = sort {
        sort_results(entries, sort);
    }
    if let Some(count) = count {
        entries.truncate(count);
    }
}

fn geo_result_for_entry(
    center: (f64, f64),
    shape: &GeoShape,
    member: String,
    raw_score: f64,
) -> Option<GeoResult> {
    let score = raw_score as u64;
    let (lon, lat) = decode_score(score);
    let distance_m = distance_m(center, (lon, lat));
    shape_contains(shape, center, (lon, lat), distance_m).then_some(GeoResult {
        member,
        score,
        lon,
        lat,
        distance_m,
    })
}

fn shape_contains(shape: &GeoShape, center: (f64, f64), point: (f64, f64), distance: f64) -> bool {
    match shape {
        GeoShape::Radius { meters, .. } => distance <= *meters,
        GeoShape::Box {
            width_m, height_m, ..
        } => {
            let horizontal = distance_m((center.0, point.1), point);
            let vertical = distance_m((point.0, center.1), point);
            horizontal <= *width_m / 2.0 && vertical <= *height_m / 2.0
        }
    }
}

fn render_search_entry(entry: GeoResult, options: &SearchOptions, unit_factor: f64) -> Frame {
    if !options.withdist && !options.withhash && !options.withcoord {
        return Frame::bulk_string(entry.member);
    }
    let mut parts = vec![Frame::bulk_string(entry.member)];
    if options.withdist {
        parts.push(Frame::bulk_string(format!(
            "{:.4}",
            entry.distance_m / unit_factor
        )));
    }
    if options.withhash {
        parts.push(Frame::Integer(entry.score as i64));
    }
    if options.withcoord {
        parts.push(Frame::Array(vec![bulk_f(entry.lon), bulk_f(entry.lat)]));
    }
    Frame::Array(parts)
}

fn store_entries(
    db: &Db,
    store: &GeoStore,
    entries: &[GeoResult],
    unit_factor: f64,
) -> Result<Frame, Error> {
    let stored = entries
        .iter()
        .map(|entry| {
            (
                entry.member.clone(),
                if store.dist {
                    entry.distance_m / unit_factor
                } else {
                    entry.score as f64
                },
            )
        })
        .collect::<Vec<_>>();
    db.zset_store_entries(&store.dest, stored)
        .map(|n| Frame::Integer(n as i64))
}

async fn store_entries_async(
    db: &Db,
    store: &GeoStore,
    entries: &[GeoResult],
    unit_factor: f64,
) -> Result<Frame, Error> {
    let stored = entries
        .iter()
        .map(|entry| {
            (
                entry.member.clone(),
                if store.dist {
                    entry.distance_m / unit_factor
                } else {
                    entry.score as f64
                },
            )
        })
        .collect::<Vec<_>>();
    db.zset_store_entries_async(&store.dest, stored)
        .await
        .map(|n| Frame::Integer(n as i64))
}
