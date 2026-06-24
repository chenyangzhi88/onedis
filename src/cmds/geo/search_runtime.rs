fn search_entries(db: &Db, search: &Geosearch) -> Result<Vec<GeoResult>, Error> {
    let center = match &search.center {
        GeoCenter::Coord(lon, lat) => (*lon, *lat),
        GeoCenter::Member(member) => db
            .zset_score(&search.key, member)?
            .map(|score| decode_score(score as u64))
            .ok_or_else(|| Error::msg("ERR could not decode requested zset member"))?,
    };
    if let Some(limit) = count_any_limit(&search.options) {
        return db
            .zset_filter_entries_limited(&search.key, limit, |_, raw_score| {
                let score = raw_score as u64;
                let (lon, lat) = decode_score(score);
                let distance_m = distance_m(center, (lon, lat));
                shape_contains(&search.shape, center, (lon, lat), distance_m)
            })
            .map(|entries| {
                entries
                    .into_iter()
                    .filter_map(|(member, raw_score)| {
                        geo_result_for_entry(center, &search.shape, member, raw_score)
                    })
                    .collect()
            });
    }

    let mut entries = db
        .zset_all_entries(&search.key)?
        .into_iter()
        .filter_map(|(member, raw_score)| {
            geo_result_for_entry(center, &search.shape, member, raw_score)
        })
        .collect::<Vec<_>>();

    if let Some(sort) = search.options.sort {
        entries.sort_by(|a, b| {
            let ord = a
                .distance_m
                .partial_cmp(&b.distance_m)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.member.cmp(&b.member));
            match sort {
                GeoSort::Asc => ord,
                GeoSort::Desc => ord.reverse(),
            }
        });
    }
    if let Some(count) = search.options.count {
        entries.truncate(count);
    }
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
    if let Some(limit) = count_any_limit(&search.options) {
        return db
            .zset_filter_entries_limited_async(&search.key, limit, |_, raw_score| {
                let score = raw_score as u64;
                let (lon, lat) = decode_score(score);
                let distance_m = distance_m(center, (lon, lat));
                shape_contains(&search.shape, center, (lon, lat), distance_m)
            })
            .await
            .map(|entries| {
                entries
                    .into_iter()
                    .filter_map(|(member, raw_score)| {
                        geo_result_for_entry(center, &search.shape, member, raw_score)
                    })
                    .collect()
            });
    }

    let mut entries = db
        .zset_all_entries_async(&search.key)
        .await?
        .into_iter()
        .filter_map(|(member, raw_score)| {
            geo_result_for_entry(center, &search.shape, member, raw_score)
        })
        .collect::<Vec<_>>();

    if let Some(sort) = search.options.sort {
        entries.sort_by(|a, b| {
            let ord = a
                .distance_m
                .partial_cmp(&b.distance_m)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.member.cmp(&b.member));
            match sort {
                GeoSort::Asc => ord,
                GeoSort::Desc => ord.reverse(),
            }
        });
    }
    if let Some(count) = search.options.count {
        entries.truncate(count);
    }
    Ok(entries)
}

fn count_any_limit(options: &SearchOptions) -> Option<usize> {
    options
        .count
        .filter(|_| options.count_any && options.sort.is_none())
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
