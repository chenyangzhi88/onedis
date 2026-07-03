use anyhow::Error;

use crate::{
    cmds::{
        bitmap::{
            bitcount::Bitcount, bitfield::Bitfield, bitop::Bitop, bitpos::Bitpos, getbit::Getbit,
            setbit::Setbit,
        },
        connect::{auth::Auth, client::Client, echo::Echo, ping::Ping, select::Select},
        full_text::{
            FtAggregate, FtAliasAdd, FtAliasDel, FtAliasUpdate, FtAlter, FtConfig, FtCreate,
            FtCursor, FtDict, FtDropIndex, FtExplain, FtHybrid, FtInfo, FtList, FtProfile,
            FtSearch, FtSpellCheck, FtSug, FtSyn, FtTagVals, FtUnsupported,
        },
        geo::{
            Geoadd, Geodist, Geohash, Geopos, Georadius, Georadiusbymember, Geosearch,
            Geosearchstore,
        },
        hash::{
            hdel::Hdel, hexists::Hexists, hexpire::Hexpire, hget::Hget, hgetall::Hgetall,
            hgetdel::Hgetdel, hgetex::Hgetex, hincrby::Hincrby, hincrbyfloat::HincrbyFloat,
            hkeys::Hkeys, hlen::Hlen, hmget::Hmget, hmset::Hmset, hpersist::Hpersist,
            hrandfield::Hrandfield, hscan::Hscan, hset::Hset, hsetex::Hsetex, hsetnx::Hsetnx,
            hstrlen::Hstrlen, httl::Httl, hvals::Hvals,
        },
        hll::{pfadd::Pfadd, pfcount::Pfcount, pfmerge::Pfmerge},
        json::{JsonDel, JsonGet, JsonSet, JsonType},
        key::{
            copy::Copy, del::Del, exists::Exists, expire::Expire, expireat::ExpireAt,
            expiretime::ExpireTime, keys::Keys, r#move::Move, persist::Persist, pexpire::Pexpire,
            pexpireat::PexpireAt, pexpiretime::PexpireTime, pttl::Pttl, randomkey::RandomKey,
            rename::Rename, renamenx::Renamenx, scan::Scan, touch::Touch, ttl::Ttl, r#type::Type,
            unlink::Unlink,
        },
        listing::{
            blmove::Blmove, blmpop::Blmpop, blpop::Blpop, brpop::Brpop, brpoplpush::Brpoplpush,
            lindex::Lindex, linsert::Linsert, llen::Llen, lmove::Lmove, lmpop::Lmpop, lpop::Lpop,
            lpos::Lpos, lpush::Lpush, lpushx::Lpushx, lrange::Lrange, lrem::Lrem, lset::Lset,
            ltrim::Ltrim, rpop::Rpop, rpoplpush::Rpoplpush, rpush::Rpush, rpushx::Rpushx,
        },
        lua::LuaCommand,
        server::{
            bgsave::Bgsave, config::Config, dbsize::Dbsize, flushall::Flushall, flushdb::Flushdb,
            info::Info, save::Save,
        },
        set::{
            sadd::Sadd, scard::Scard, sdiff::Sdiff, sdiffstore::Sdiffstore, sinter::Sinter,
            sintercard::Sintercard, sinterstore::Sinterstore, sismember::Sismember,
            smembers::Smembers, smismember::Smismember, smove::Smove, spop::Spop,
            srandmember::Srandmember, srem::Srem, sscan::Sscan, sunion::Sunion,
            sunionstore::Sunionstore,
        },
        sorted_set::{
            bzmpop::Bzmpop, bzpopmax::Bzpopmax, bzpopmin::Bzpopmin, zadd::Zadd, zcard::Zcard,
            zcount::Zcount, zdiff::Zdiff, zdiffstore::Zdiffstore, zincrby::Zincrby, zinter::Zinter,
            zintercard::Zintercard, zinterstore::Zinterstore, zlexcount::Zlexcount, zmpop::Zmpop,
            zmscore::Zmscore, zpopmax::Zpopmax, zpopmin::Zpopmin, zrandmember::Zrandmember,
            zrange::Zrange, zrangebylex::Zrangebylex, zrangebyscore::Zrangebyscore,
            zrangestore::Zrangestore, zrank::Zrank, zrem::Zrem, zremrangebylex::Zremrangebylex,
            zremrangebyrank::Zremrangebyrank, zremrangebyscore::Zremrangebyscore,
            zrevrange::Zrevrange, zrevrangebylex::Zrevrangebylex,
            zrevrangebyscore::Zrevrangebyscore, zrevrank::Zrevrank, zscan::Zscan, zscore::Zscore,
            zunion::Zunion, zunionstore::Zunionstore,
        },
        stream::{
            xack::Xack, xackdel::Xackdel, xadd::Xadd, xautoclaim::Xautoclaim, xcfgset::Xcfgset,
            xclaim::Xclaim, xdel::Xdel, xdelex::Xdelex, xgroup::Xgroup, xinfo::Xinfo, xlen::Xlen,
            xpending::Xpending, xrange::Xrange, xread::Xread, xreadgroup::Xreadgroup,
            xrevrange::Xrevrange, xsetid::Xsetid, xtrim::Xtrim,
        },
        string::{
            append::Append, decr::Decr, decrby::Decrby, get::Get, getdel::GetDel, getex::GetEx,
            getrange::GetRange, getset::GetSet, incr::Incr, incrby::Incrby,
            incrbyfloat::IncrbyFloat, lcs::Lcs, mget::Mget, mset::Mset, msetex::Msetex,
            msetnx::Msetnx, psetex::Psetex, set::Set, setex::Setex, setnx::Setnx,
            setrange::SetRange, strlen::Strlen,
        },
        transaction::{discard::Discard, exec::Exec, multi::Multi, unwatch::Unwatch, watch::Watch},
        unknown::Unknown,
        vector::{
            VAdd, VCard, VDim, VEmb, VGetAttr, VInfo, VLinks, VRandMember, VRem, VSetAttr, VSim,
        },
        wasm::WasmCommand,
    },
    frame::Frame,
};

mod aof;
pub mod dispatch;
mod kind;
mod names;
mod parse;

pub use kind::Command;

#[cfg(test)]
mod tests;
