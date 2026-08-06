use super::*;

type CommandFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Frame, Error>> + Send + 'a>>;

fn box_command<'a, C, F, Fut>(command: C, db: &'a Db, apply: F) -> CommandFuture<'a>
where
    F: FnOnce(C, &'a Db) -> Fut,
    Fut: std::future::Future<Output = Result<Frame, Error>> + Send + 'a,
{
    Box::pin(apply(command, db))
}

pub fn handle_command_async(db: &Db, command: Command) -> CommandFuture<'_> {
    match command {
        Command::Bitcount(bitcount) => {
            box_command(bitcount, db, |bitcount, db| bitcount.apply_async(db))
        }
        Command::Set(set) => box_command(set, db, |set, db| set.apply_async(db)),
        Command::Bitfield(bitfield) => {
            box_command(bitfield, db, |bitfield, db| bitfield.apply_async(db))
        }
        Command::Bitop(bitop) => box_command(bitop, db, |bitop, db| bitop.apply_async(db)),
        Command::Bitpos(bitpos) => box_command(bitpos, db, |bitpos, db| bitpos.apply_async(db)),
        Command::Get(get) => box_command(get, db, |get, db| get.apply_async(db)),
        Command::Getbit(getbit) => box_command(getbit, db, |getbit, db| getbit.apply_async(db)),
        Command::GetRange(getrange) => {
            box_command(getrange, db, |getrange, db| getrange.apply_async(db))
        }
        Command::Lcs(lcs) => box_command(lcs, db, |lcs, db| lcs.apply_async(db)),
        Command::Setex(setex) => box_command(setex, db, |setex, db| setex.apply_async(db)),
        Command::Setnx(setnx) => box_command(setnx, db, |setnx, db| setnx.apply_async(db)),
        Command::Psetex(psetex) => box_command(psetex, db, |psetex, db| psetex.apply_async(db)),
        Command::Mset(mset) => box_command(mset, db, |mset, db| mset.apply_async(db)),
        Command::Mget(mget) => box_command(mget, db, |mget, db| mget.apply_async(db)),
        Command::Msetnx(msetnx) => box_command(msetnx, db, |msetnx, db| msetnx.apply_async(db)),
        Command::Incr(incr) => box_command(incr, db, |incr, db| incr.apply_async(db)),
        Command::Incrby(incrby) => box_command(incrby, db, |incrby, db| incrby.apply_async(db)),
        Command::Decr(decr) => box_command(decr, db, |decr, db| decr.apply_async(db)),
        Command::Decrby(decrby) => box_command(decrby, db, |decrby, db| decrby.apply_async(db)),
        Command::Append(append) => box_command(append, db, |append, db| append.apply_async(db)),
        Command::SetRange(setrange) => {
            box_command(setrange, db, |setrange, db| setrange.apply_async(db))
        }
        Command::Setbit(setbit) => box_command(setbit, db, |setbit, db| setbit.apply_async(db)),
        Command::GetSet(getset) => box_command(getset, db, |getset, db| getset.apply_async(db)),
        Command::GetDel(getdel) => box_command(getdel, db, |getdel, db| getdel.apply_async(db)),
        Command::GetEx(getex) => box_command(getex, db, |getex, db| getex.apply_async(db)),
        Command::Msetex(msetex) => box_command(msetex, db, |msetex, db| msetex.apply_async(db)),
        Command::Strlen(strlen) => box_command(strlen, db, |strlen, db| strlen.apply_async(db)),
        Command::IncrbyFloat(incrbyfloat) => box_command(incrbyfloat, db, |incrbyfloat, db| {
            incrbyfloat.apply_async(db)
        }),
        Command::Hset(hset) => box_command(hset, db, |hset, db| hset.apply_async(db)),
        Command::Hdel(hdel) => box_command(hdel, db, |hdel, db| hdel.apply_async(db)),
        Command::Hexists(hexists) => {
            box_command(hexists, db, |hexists, db| hexists.apply_async(db))
        }
        Command::Hget(hget) => box_command(hget, db, |hget, db| hget.apply_async(db)),
        Command::Hgetall(hgetall) => {
            box_command(hgetall, db, |hgetall, db| hgetall.apply_async(db))
        }
        Command::Hkeys(hkeys) => box_command(hkeys, db, |hkeys, db| hkeys.apply_async(db)),
        Command::Hlen(hlen) => box_command(hlen, db, |hlen, db| hlen.apply_async(db)),
        Command::Hmget(hmget) => box_command(hmget, db, |hmget, db| hmget.apply_async(db)),
        Command::Hrandfield(hrandfield) => {
            box_command(hrandfield, db, |hrandfield, db| hrandfield.apply_async(db))
        }
        Command::Hscan(hscan) => box_command(hscan, db, |hscan, db| hscan.apply_async(db)),
        Command::Hstrlen(hstrlen) => {
            box_command(hstrlen, db, |hstrlen, db| hstrlen.apply_async(db))
        }
        Command::Httl(httl) => box_command(httl, db, |httl, db| httl.apply_async(db)),
        Command::Hpttl(hpttl) => box_command(hpttl, db, |hpttl, db| hpttl.apply_async(db)),
        Command::HexpireTime(hexpiretime) => box_command(hexpiretime, db, |hexpiretime, db| {
            hexpiretime.apply_async(db)
        }),
        Command::HpexpireTime(hpexpiretime) => box_command(hpexpiretime, db, |hpexpiretime, db| {
            hpexpiretime.apply_async(db)
        }),
        Command::Hvals(hvals) => box_command(hvals, db, |hvals, db| hvals.apply_async(db)),
        Command::Hmset(hmset) => box_command(hmset, db, |hmset, db| hmset.apply_async(db)),
        Command::Hsetnx(hsetnx) => box_command(hsetnx, db, |hsetnx, db| hsetnx.apply_async(db)),
        Command::Hincrby(hincrby) => {
            box_command(hincrby, db, |hincrby, db| hincrby.apply_async(db))
        }
        Command::HincrbyFloat(hincrbyfloat) => box_command(hincrbyfloat, db, |hincrbyfloat, db| {
            hincrbyfloat.apply_async(db)
        }),
        Command::Hgetdel(hgetdel) => {
            box_command(hgetdel, db, |hgetdel, db| hgetdel.apply_async(db))
        }
        Command::Hgetex(hgetex) => box_command(hgetex, db, |hgetex, db| hgetex.apply_async(db)),
        Command::Hsetex(hsetex) => box_command(hsetex, db, |hsetex, db| hsetex.apply_async(db)),
        Command::Hexpire(hexpire) => {
            box_command(hexpire, db, |hexpire, db| hexpire.apply_async(db))
        }
        Command::HexpireAt(hexpireat) => {
            box_command(hexpireat, db, |hexpireat, db| hexpireat.apply_async(db))
        }
        Command::Hpexpire(hpexpire) => {
            box_command(hpexpire, db, |hpexpire, db| hpexpire.apply_async(db))
        }
        Command::HpexpireAt(hpexpireat) => {
            box_command(hpexpireat, db, |hpexpireat, db| hpexpireat.apply_async(db))
        }
        Command::Hpersist(hpersist) => {
            box_command(hpersist, db, |hpersist, db| hpersist.apply_async(db))
        }
        Command::Del(del) => box_command(del, db, |del, db| del.apply_async(db)),
        Command::Unlink(unlink) => box_command(unlink, db, |unlink, db| unlink.apply_async(db)),
        Command::Expire(expire) => box_command(expire, db, |expire, db| expire.apply_async(db)),
        Command::ExpireAt(expireat) => {
            box_command(expireat, db, |expireat, db| expireat.apply_async(db))
        }
        Command::Pexpire(pexpire) => {
            box_command(pexpire, db, |pexpire, db| pexpire.apply_async(db))
        }
        Command::PexpireAt(pexpireat) => {
            box_command(pexpireat, db, |pexpireat, db| pexpireat.apply_async(db))
        }
        Command::Persist(persist) => {
            box_command(persist, db, |persist, db| persist.apply_async(db))
        }
        Command::Rename(rename) => box_command(rename, db, |rename, db| rename.apply_async(db)),
        Command::Renamenx(renamenx) => {
            box_command(renamenx, db, |renamenx, db| renamenx.apply_async(db))
        }
        Command::Flushdb(flushdb) => {
            box_command(flushdb, db, |flushdb, db| flushdb.apply_async(db))
        }
        Command::Exists(exists) => box_command(exists, db, |exists, db| exists.apply_async(db)),
        Command::ExpireTime(expiretime) => {
            box_command(expiretime, db, |expiretime, db| expiretime.apply_async(db))
        }
        Command::PexpireTime(pexpiretime) => box_command(pexpiretime, db, |pexpiretime, db| {
            pexpiretime.apply_async(db)
        }),
        Command::RandomKey(randomkey) => {
            box_command(randomkey, db, |randomkey, db| randomkey.apply_async(db))
        }
        Command::Touch(touch) => box_command(touch, db, |touch, db| touch.apply_async(db)),
        Command::Ttl(ttl) => box_command(ttl, db, |ttl, db| ttl.apply_async(db)),
        Command::Pttl(pttl) => box_command(pttl, db, |pttl, db| pttl.apply_async(db)),
        Command::Type(r#type) => box_command(r#type, db, |r#type, db| r#type.apply_async(db)),
        Command::Lrange(lrange) => box_command(lrange, db, |lrange, db| lrange.apply_async(db)),
        Command::Dbsize(dbsize) => box_command(dbsize, db, |dbsize, db| dbsize.apply_async(db)),
        Command::Keys(keys) => box_command(keys, db, |keys, db| keys.apply_async(db)),
        Command::Scan(scan) => box_command(scan, db, |scan, db| scan.apply_async(db)),
        Command::Sdiff(sdiff) => box_command(sdiff, db, |sdiff, db| sdiff.apply_async(db)),
        Command::Sdiffstore(sdiffstore) => {
            box_command(sdiffstore, db, |sdiffstore, db| sdiffstore.apply_async(db))
        }
        Command::Sadd(sadd) => box_command(sadd, db, |sadd, db| sadd.apply_async(db)),
        Command::Scard(scard) => box_command(scard, db, |scard, db| scard.apply_async(db)),
        Command::Sismember(sismember) => {
            box_command(sismember, db, |sismember, db| sismember.apply_async(db))
        }
        Command::Sintercard(sintercard) => {
            box_command(sintercard, db, |sintercard, db| sintercard.apply_async(db))
        }
        Command::Smismember(smismember) => {
            box_command(smismember, db, |smismember, db| smismember.apply_async(db))
        }
        Command::Srem(srem) => box_command(srem, db, |srem, db| srem.apply_async(db)),
        Command::Sinter(sinter) => box_command(sinter, db, |sinter, db| sinter.apply_async(db)),
        Command::Sinterstore(sinterstore) => box_command(sinterstore, db, |sinterstore, db| {
            sinterstore.apply_async(db)
        }),
        Command::Smembers(smembers) => {
            box_command(smembers, db, |smembers, db| smembers.apply_async(db))
        }
        Command::Spop(spop) => box_command(spop, db, |spop, db| spop.apply_async(db)),
        Command::Srandmember(srandmember) => box_command(srandmember, db, |srandmember, db| {
            srandmember.apply_async(db)
        }),
        Command::Sscan(sscan) => box_command(sscan, db, |sscan, db| sscan.apply_async(db)),
        Command::Sunion(sunion) => box_command(sunion, db, |sunion, db| sunion.apply_async(db)),
        Command::Sunionstore(sunionstore) => box_command(sunionstore, db, |sunionstore, db| {
            sunionstore.apply_async(db)
        }),
        Command::Zcard(zcard) => box_command(zcard, db, |zcard, db| zcard.apply_async(db)),
        Command::Zadd(zadd) => box_command(zadd, db, |zadd, db| zadd.apply_async(db)),
        Command::Zincrby(zincrby) => {
            box_command(zincrby, db, |zincrby, db| zincrby.apply_async(db))
        }
        Command::Zcount(zcount) => box_command(zcount, db, |zcount, db| zcount.apply_async(db)),
        Command::Zdiff(zdiff) => box_command(zdiff, db, |zdiff, db| zdiff.apply_async(db)),
        Command::Zrange(zrange) => box_command(zrange, db, |zrange, db| zrange.apply_async(db)),
        Command::Zrangebylex(zrangebylex) => box_command(zrangebylex, db, |zrangebylex, db| {
            zrangebylex.apply_async(db)
        }),
        Command::Zrank(zrank) => box_command(zrank, db, |zrank, db| zrank.apply_async(db)),
        Command::Zrem(zrem) => box_command(zrem, db, |zrem, db| zrem.apply_async(db)),
        Command::Zremrangebyrank(zremrangebyrank) => {
            box_command(zremrangebyrank, db, |zremrangebyrank, db| {
                zremrangebyrank.apply_async(db)
            })
        }
        Command::Zremrangebyscore(zremrangebyscore) => {
            box_command(zremrangebyscore, db, |zremrangebyscore, db| {
                zremrangebyscore.apply_async(db)
            })
        }
        Command::Zdiffstore(zdiffstore) => {
            box_command(zdiffstore, db, |zdiffstore, db| zdiffstore.apply_async(db))
        }
        Command::Zinter(zinter) => box_command(zinter, db, |zinter, db| zinter.apply_async(db)),
        Command::Zintercard(zintercard) => {
            box_command(zintercard, db, |zintercard, db| zintercard.apply_async(db))
        }
        Command::Zinterstore(zinterstore) => box_command(zinterstore, db, |zinterstore, db| {
            zinterstore.apply_async(db)
        }),
        Command::Zlexcount(zlexcount) => {
            box_command(zlexcount, db, |zlexcount, db| zlexcount.apply_async(db))
        }
        Command::Zmscore(zmscore) => {
            box_command(zmscore, db, |zmscore, db| zmscore.apply_async(db))
        }
        Command::Zrandmember(zrandmember) => box_command(zrandmember, db, |zrandmember, db| {
            zrandmember.apply_async(db)
        }),
        Command::Zunionstore(zunionstore) => box_command(zunionstore, db, |zunionstore, db| {
            zunionstore.apply_async(db)
        }),
        Command::Zpopmax(zpopmax) => {
            box_command(zpopmax, db, |zpopmax, db| zpopmax.apply_async(db))
        }
        Command::Zpopmin(zpopmin) => {
            box_command(zpopmin, db, |zpopmin, db| zpopmin.apply_async(db))
        }
        Command::Zremrangebylex(zremrangebylex) => {
            box_command(zremrangebylex, db, |zremrangebylex, db| {
                zremrangebylex.apply_async(db)
            })
        }
        Command::Zrevrange(zrevrange) => {
            box_command(zrevrange, db, |zrevrange, db| zrevrange.apply_async(db))
        }
        Command::Zrevrangebylex(zrevrangebylex) => {
            box_command(zrevrangebylex, db, |zrevrangebylex, db| {
                zrevrangebylex.apply_async(db)
            })
        }
        Command::Zrevrangebyscore(zrevrangebyscore) => {
            box_command(zrevrangebyscore, db, |zrevrangebyscore, db| {
                zrevrangebyscore.apply_async(db)
            })
        }
        Command::Zrevrank(zrevrank) => {
            box_command(zrevrank, db, |zrevrank, db| zrevrank.apply_async(db))
        }
        Command::Zrangebyscore(zrangebyscore) => {
            box_command(zrangebyscore, db, |zrangebyscore, db| {
                zrangebyscore.apply_async(db)
            })
        }
        Command::Zrangestore(zrangestore) => box_command(zrangestore, db, |zrangestore, db| {
            zrangestore.apply_async(db)
        }),
        Command::Zscan(zscan) => box_command(zscan, db, |zscan, db| zscan.apply_async(db)),
        Command::Zscore(zscore) => box_command(zscore, db, |zscore, db| zscore.apply_async(db)),
        Command::Zunion(zunion) => box_command(zunion, db, |zunion, db| zunion.apply_async(db)),
        Command::Lindex(lindex) => box_command(lindex, db, |lindex, db| lindex.apply_async(db)),
        Command::Llen(llen) => box_command(llen, db, |llen, db| llen.apply_async(db)),
        Command::Lpos(lpos) => box_command(lpos, db, |lpos, db| lpos.apply_async(db)),
        Command::Lpush(lpush) => box_command(lpush, db, |lpush, db| lpush.apply_async(db)),
        Command::Lpushx(lpushx) => box_command(lpushx, db, |lpushx, db| lpushx.apply_async(db)),
        Command::Rpush(rpush) => box_command(rpush, db, |rpush, db| rpush.apply_async(db)),
        Command::Rpushx(rpushx) => box_command(rpushx, db, |rpushx, db| rpushx.apply_async(db)),
        Command::Lpop(lpop) => box_command(lpop, db, |lpop, db| lpop.apply_async(db)),
        Command::Rpop(rpop) => box_command(rpop, db, |rpop, db| rpop.apply_async(db)),
        Command::Lset(lset) => box_command(lset, db, |lset, db| lset.apply_async(db)),
        Command::Ltrim(ltrim) => box_command(ltrim, db, |ltrim, db| ltrim.apply_async(db)),
        Command::Linsert(linsert) => {
            box_command(linsert, db, |linsert, db| linsert.apply_async(db))
        }
        Command::Lrem(lrem) => box_command(lrem, db, |lrem, db| lrem.apply_async(db)),
        Command::Lmpop(lmpop) => box_command(lmpop, db, |lmpop, db| lmpop.apply_async(db)),
        Command::Blpop(blpop) => box_command(blpop, db, |blpop, db| blpop.apply_async(db)),
        Command::Brpop(brpop) => box_command(brpop, db, |brpop, db| brpop.apply_async(db)),
        Command::Brpoplpush(brpoplpush) => {
            box_command(brpoplpush, db, |brpoplpush, db| brpoplpush.apply_async(db))
        }
        Command::Blmove(blmove) => box_command(blmove, db, |blmove, db| blmove.apply_async(db)),
        Command::Blmpop(blmpop) => box_command(blmpop, db, |blmpop, db| blmpop.apply_async(db)),
        Command::Rpoplpush(rpoplpush) => {
            box_command(rpoplpush, db, |rpoplpush, db| rpoplpush.apply_async(db))
        }
        Command::Lmove(lmove) => box_command(lmove, db, |lmove, db| lmove.apply_async(db)),
        Command::Smove(smove) => box_command(smove, db, |smove, db| smove.apply_async(db)),
        Command::Geoadd(geoadd) => box_command(geoadd, db, |geoadd, db| geoadd.apply_async(db)),
        Command::Geodist(geodist) => {
            box_command(geodist, db, |geodist, db| geodist.apply_async(db))
        }
        Command::Geohash(geohash) => {
            box_command(geohash, db, |geohash, db| geohash.apply_async(db))
        }
        Command::Geopos(geopos) => box_command(geopos, db, |geopos, db| geopos.apply_async(db)),
        Command::Geosearch(geosearch) => {
            box_command(geosearch, db, |geosearch, db| geosearch.apply_async(db))
        }
        Command::Georadius(georadius) => {
            box_command(georadius, db, |georadius, db| georadius.apply_async(db))
        }
        Command::Georadiusbymember(georadiusbymember) => {
            box_command(georadiusbymember, db, |georadiusbymember, db| {
                georadiusbymember.apply_async(db)
            })
        }
        Command::Geosearchstore(geosearchstore) => {
            box_command(geosearchstore, db, |geosearchstore, db| {
                geosearchstore.apply_async(db)
            })
        }
        Command::Pfadd(pfadd) => box_command(pfadd, db, |pfadd, db| pfadd.apply_async(db)),
        Command::Pfcount(pfcount) => {
            box_command(pfcount, db, |pfcount, db| pfcount.apply_async(db))
        }
        Command::Pfmerge(pfmerge) => {
            box_command(pfmerge, db, |pfmerge, db| pfmerge.apply_async(db))
        }
        Command::Xadd(xadd) => box_command(xadd, db, |xadd, db| xadd.apply_async(db)),
        Command::Xdel(xdel) => box_command(xdel, db, |xdel, db| xdel.apply_async(db)),
        Command::Xtrim(xtrim) => box_command(xtrim, db, |xtrim, db| xtrim.apply_async(db)),
        Command::Xack(xack) => box_command(xack, db, |xack, db| xack.apply_async(db)),
        Command::Xackdel(xackdel) => {
            box_command(xackdel, db, |xackdel, db| xackdel.apply_async(db))
        }
        Command::Xautoclaim(xautoclaim) => {
            box_command(xautoclaim, db, |xautoclaim, db| xautoclaim.apply_async(db))
        }
        Command::Xcfgset(xcfgset) => {
            box_command(xcfgset, db, |xcfgset, db| xcfgset.apply_async(db))
        }
        Command::Xclaim(xclaim) => box_command(xclaim, db, |xclaim, db| xclaim.apply_async(db)),
        Command::Xdelex(xdelex) => box_command(xdelex, db, |xdelex, db| xdelex.apply_async(db)),
        Command::Xgroup(xgroup) => box_command(xgroup, db, |xgroup, db| xgroup.apply_async(db)),
        Command::Xinfo(xinfo) => box_command(xinfo, db, |xinfo, db| xinfo.apply_async(db)),
        Command::Xlen(xlen) => box_command(xlen, db, |xlen, db| xlen.apply_async(db)),
        Command::Xpending(xpending) => {
            box_command(xpending, db, |xpending, db| xpending.apply_async(db))
        }
        Command::Xrange(xrange) => box_command(xrange, db, |xrange, db| xrange.apply_async(db)),
        Command::Xread(xread) => box_command(xread, db, |xread, db| xread.apply_async(db)),
        Command::Xsetid(xsetid) => box_command(xsetid, db, |xsetid, db| xsetid.apply_async(db)),
        Command::Xrevrange(xrevrange) => {
            box_command(xrevrange, db, |xrevrange, db| xrevrange.apply_async(db))
        }
        Command::Xreadgroup(xreadgroup) => {
            box_command(xreadgroup, db, |xreadgroup, db| xreadgroup.apply_async(db))
        }
        Command::Zmpop(zmpop) => box_command(zmpop, db, |zmpop, db| zmpop.apply_async(db)),
        Command::Bzpopmin(bzpopmin) => {
            box_command(bzpopmin, db, |bzpopmin, db| bzpopmin.apply_async(db))
        }
        Command::Bzpopmax(bzpopmax) => {
            box_command(bzpopmax, db, |bzpopmax, db| bzpopmax.apply_async(db))
        }
        Command::Bzmpop(bzmpop) => box_command(bzmpop, db, |bzmpop, db| bzmpop.apply_async(db)),
        Command::JsonSet(json_set) => {
            box_command(json_set, db, |json_set, db| json_set.apply_async(db))
        }
        Command::JsonGet(json_get) => {
            box_command(json_get, db, |json_get, db| json_get.apply_async(db))
        }
        Command::JsonDel(json_del) => {
            box_command(json_del, db, |json_del, db| json_del.apply_async(db))
        }
        Command::JsonType(json_type) => {
            box_command(json_type, db, |json_type, db| json_type.apply_async(db))
        }
        Command::FtCreate(ft_create) => {
            box_command(ft_create, db, |ft_create, db| ft_create.apply_async(db))
        }
        Command::FtList(ft_list) => box_command(ft_list, db, |ft_list, db| ft_list.apply_async(db)),
        Command::FtDropIndex(ft_drop_index) => {
            box_command(ft_drop_index, db, |ft_drop_index, db| {
                ft_drop_index.apply_async(db)
            })
        }
        Command::FtAlter(ft_alter) => {
            box_command(ft_alter, db, |ft_alter, db| ft_alter.apply_async(db))
        }
        Command::FtAliasAdd(ft_alias_add) => box_command(ft_alias_add, db, |ft_alias_add, db| {
            ft_alias_add.apply_async(db)
        }),
        Command::FtAliasUpdate(ft_alias_update) => {
            box_command(ft_alias_update, db, |ft_alias_update, db| {
                ft_alias_update.apply_async(db)
            })
        }
        Command::FtAliasDel(ft_alias_del) => box_command(ft_alias_del, db, |ft_alias_del, db| {
            ft_alias_del.apply_async(db)
        }),
        Command::FtConfig(ft_config) => {
            box_command(ft_config, db, |ft_config, db| ft_config.apply_async(db))
        }
        Command::FtInfo(ft_info) => box_command(ft_info, db, |ft_info, db| ft_info.apply_async(db)),
        Command::FtSearch(ft_search) => {
            box_command(ft_search, db, |ft_search, db| ft_search.apply_async(db))
        }
        Command::FtHybrid(ft_hybrid) => {
            box_command(ft_hybrid, db, |ft_hybrid, db| ft_hybrid.apply_async(db))
        }
        Command::FtAggregate(ft_aggregate) => box_command(ft_aggregate, db, |ft_aggregate, db| {
            ft_aggregate.apply_async(db)
        }),
        Command::FtCursor(ft_cursor) => {
            box_command(ft_cursor, db, |ft_cursor, db| ft_cursor.apply_async(db))
        }
        Command::FtProfile(ft_profile) => {
            box_command(ft_profile, db, |ft_profile, db| ft_profile.apply_async(db))
        }
        Command::FtExplain(ft_explain) => {
            box_command(ft_explain, db, |ft_explain, db| ft_explain.apply_async(db))
        }
        Command::FtTagVals(ft_tagvals) => {
            box_command(ft_tagvals, db, |ft_tagvals, db| ft_tagvals.apply_async(db))
        }
        Command::FtDict(ft_dict) => box_command(ft_dict, db, |ft_dict, db| ft_dict.apply_async(db)),
        Command::FtSpellCheck(ft_spellcheck) => {
            box_command(ft_spellcheck, db, |ft_spellcheck, db| {
                ft_spellcheck.apply_async(db)
            })
        }
        Command::FtSug(ft_sug) => box_command(ft_sug, db, |ft_sug, db| ft_sug.apply_async(db)),
        Command::FtSyn(ft_syn) => box_command(ft_syn, db, |ft_syn, db| ft_syn.apply_async(db)),
        Command::FtUnsupported(ft_unsupported) => {
            box_command(ft_unsupported, db, |ft_unsupported, _| {
                ft_unsupported.apply_async()
            })
        }
        Command::Lua(lua) => box_command(lua, db, |lua, db| lua.apply_async(db)),
        Command::VAdd(vadd) => box_command(vadd, db, |vadd, db| vadd.apply_async(db)),
        Command::VSim(vsim) => box_command(vsim, db, |vsim, db| vsim.apply_async(db)),
        Command::VRem(vrem) => box_command(vrem, db, |vrem, db| vrem.apply_async(db)),
        Command::VCard(vcard) => box_command(vcard, db, |vcard, db| vcard.apply_async(db)),
        Command::VDim(vdim) => box_command(vdim, db, |vdim, db| vdim.apply_async(db)),
        Command::VEmb(vemb) => box_command(vemb, db, |vemb, db| vemb.apply_async(db)),
        Command::VGetAttr(vgetattr) => {
            box_command(vgetattr, db, |vgetattr, db| vgetattr.apply_async(db))
        }
        Command::VSetAttr(vsetattr) => {
            box_command(vsetattr, db, |vsetattr, db| vsetattr.apply_async(db))
        }
        Command::VInfo(vinfo) => box_command(vinfo, db, |vinfo, db| vinfo.apply_async(db)),
        Command::VRandMember(vrandmember) => box_command(vrandmember, db, |vrandmember, db| {
            vrandmember.apply_async(db)
        }),
        Command::VLinks(vlinks) => box_command(vlinks, db, |vlinks, db| vlinks.apply_async(db)),
        Command::Copy(copy) => box_command(copy, db, |copy, db| async move {
            let copied = db
                .copy_key_to_db_async(
                    copy.db_index().unwrap_or(db.db_index() as usize) as u16,
                    copy.source(),
                    copy.destination(),
                    copy.replace(),
                )
                .await?;
            Ok(Frame::Integer(if copied { 1 } else { 0 }))
        }),
        Command::Move(r#move) => box_command(r#move, db, |r#move, db| async move {
            let moved = db
                .move_key_to_db_async(r#move.get_db_index() as u16, r#move.get_key())
                .await?;
            Ok(Frame::Integer(if moved { 1 } else { 0 }))
        }),
        Command::Flushall(_) => box_command((), db, |(), db| async move {
            db.clear_async().await;
            Ok(Frame::Ok)
        }),
        Command::Info(info) => box_command(info, db, |info, db| info.apply_async(db)),
        Command::Save(_) => box_command((), db, |(), _| async { Ok(Frame::Ok) }),
        Command::Bgsave(_) => box_command((), db, |(), _| async { Ok(Frame::Ok) }),
        other => box_command(
            other,
            db,
            |other, db| async move { handle_command(db, other) },
        ),
    }
}
