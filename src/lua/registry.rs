use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use anyhow::{Error, Result};
use sha1_smol::Sha1;

use crate::{frame::Frame, store::db::Db};

use super::runtime::run_lua_script;

#[derive(Default)]
pub struct LuaRegistry {
    scripts: Mutex<HashMap<String, String>>,
    execution: Mutex<LuaExecutionState>,
}

#[derive(Default)]
struct LuaExecutionState {
    active: bool,
    kill_requested: bool,
    write_seen: bool,
}

pub(crate) struct LuaExecutionGuard<'a> {
    registry: &'a LuaRegistry,
}

impl Drop for LuaExecutionGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut execution) = self.registry.execution.lock() {
            execution.active = false;
            execution.kill_requested = false;
            execution.write_seen = false;
        }
    }
}

pub static LUA_REGISTRY: OnceLock<LuaRegistry> = OnceLock::new();

pub fn lua_registry() -> &'static LuaRegistry {
    LUA_REGISTRY.get_or_init(LuaRegistry::default)
}

pub struct LuaEval {
    pub script: String,
    pub keys: Vec<String>,
    pub args: Vec<String>,
    pub read_only: bool,
}

impl LuaRegistry {
    pub fn load(&self, script: &str) -> Result<String> {
        let sha = sha1_hex(script);
        self.scripts
            .lock()
            .map_err(|_| Error::msg("ERR lua script cache lock poisoned"))?
            .insert(sha.clone(), script.to_string());
        Ok(sha)
    }

    pub fn get(&self, sha: &str) -> Result<Option<String>> {
        Ok(self
            .scripts
            .lock()
            .map_err(|_| Error::msg("ERR lua script cache lock poisoned"))?
            .get(sha)
            .cloned())
    }

    pub fn exists(&self, shas: &[String]) -> Result<Vec<bool>> {
        let scripts = self
            .scripts
            .lock()
            .map_err(|_| Error::msg("ERR lua script cache lock poisoned"))?;
        Ok(shas.iter().map(|sha| scripts.contains_key(sha)).collect())
    }

    pub fn flush(&self) -> Result<()> {
        self.scripts
            .lock()
            .map_err(|_| Error::msg("ERR lua script cache lock poisoned"))?
            .clear();
        Ok(())
    }

    pub fn kill(&self) -> Result<()> {
        let mut execution = self
            .execution
            .lock()
            .map_err(|_| Error::msg("ERR lua execution state lock poisoned"))?;
        if !execution.active {
            return Err(Error::msg("NOTBUSY No scripts in execution right now."));
        }
        if execution.write_seen {
            return Err(Error::msg(
                "UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.",
            ));
        }
        execution.kill_requested = true;
        Ok(())
    }

    pub fn eval(&self, db: &Db, eval: LuaEval) -> Result<Frame> {
        self.load(&eval.script)?;
        let _guard = self.begin_execution()?;
        let txn_db = Arc::new(db.transactional_view()?);
        let result = match run_lua_script(txn_db.clone(), &eval) {
            Ok(result) => result,
            Err(err) => {
                txn_db.discard_transaction();
                return Err(err);
            }
        };
        if eval.read_only {
            txn_db.discard_transaction();
        } else {
            txn_db.commit_transaction()?;
        }
        Ok(result)
    }

    pub(crate) fn begin_execution(&self) -> Result<LuaExecutionGuard<'_>> {
        let mut execution = self
            .execution
            .lock()
            .map_err(|_| Error::msg("ERR lua execution state lock poisoned"))?;
        if execution.active {
            return Err(Error::msg(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
            ));
        }
        execution.active = true;
        execution.kill_requested = false;
        execution.write_seen = false;
        Ok(LuaExecutionGuard { registry: self })
    }

    pub(crate) fn note_write(&self) -> Result<()> {
        let mut execution = self
            .execution
            .lock()
            .map_err(|_| Error::msg("ERR lua execution state lock poisoned"))?;
        if execution.active {
            execution.write_seen = true;
        }
        Ok(())
    }

    pub(crate) fn kill_requested(&self) -> Result<bool> {
        Ok(self
            .execution
            .lock()
            .map_err(|_| Error::msg("ERR lua execution state lock poisoned"))?
            .kill_requested)
    }
}

pub fn sha1_hex(script: &str) -> String {
    let mut sha = Sha1::new();
    sha.update(script.as_bytes());
    sha.digest().to_string()
}
