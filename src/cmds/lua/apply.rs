impl LuaCommand {
    pub fn apply(self, db: &Db) -> Result<Frame> {
        match self {
            Self::Eval(eval) => {
                let started = Instant::now();
                let result = lua_registry().eval(db, eval);
                global_metrics().record_lua_eval(elapsed_us(started), result.is_err());
                result
            }
            Self::EvalSha {
                sha,
                keys,
                args,
                read_only,
            } => {
                let started = Instant::now();
                let result = (|| {
                    let script = lua_registry().get(&sha)?.ok_or_else(|| {
                        Error::msg("NOSCRIPT No matching script. Please use EVAL.")
                    })?;
                    lua_registry().eval(
                        db,
                        LuaEval {
                            script,
                            keys,
                            args,
                            read_only,
                        },
                    )
                })();
                global_metrics().record_lua_eval(elapsed_us(started), result.is_err());
                result
            }
            Self::ScriptLoad(script) => Ok(Frame::bulk_string(lua_registry().load(&script)?)),
            Self::ScriptExists(shas) => Ok(Frame::Array(
                lua_registry()
                    .exists(&shas)?
                    .into_iter()
                    .map(|exists| Frame::Integer(i64::from(exists)))
                    .collect(),
            )),
            Self::ScriptFlush => {
                lua_registry().flush()?;
                Ok(Frame::Ok)
            }
            Self::ScriptKill => {
                lua_registry().kill()?;
                Ok(Frame::Ok)
            }
            Self::ScriptDebug => Ok(Frame::Ok),
            Self::ScriptHelp => Ok(Frame::Array(vec![
                Frame::bulk_string("SCRIPT LOAD script"),
                Frame::bulk_string("SCRIPT EXISTS sha [sha ...]"),
                Frame::bulk_string("SCRIPT FLUSH [ASYNC|SYNC]"),
                Frame::bulk_string("SCRIPT KILL"),
                Frame::bulk_string("SCRIPT DEBUG YES|SYNC|NO"),
            ])),
        }
    }

    pub async fn apply_async(self, db: &Db) -> Result<Frame> {
        self.apply(db)
    }
}
