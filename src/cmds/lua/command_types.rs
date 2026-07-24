pub enum LuaCommand {
    Eval(LuaEval),
    EvalSha {
        sha: String,
        keys: Vec<String>,
        args: Vec<String>,
        read_only: bool,
    },
    ScriptLoad(String),
    ScriptExists(Vec<String>),
    ScriptFlush,
    ScriptKill,
    ScriptDebug,
    ScriptHelp,
}

impl LuaCommand {
    pub(crate) fn command_name(&self) -> &'static str {
        match self {
            Self::Eval(eval) if eval.read_only => "EVAL_RO",
            Self::Eval(_) => "EVAL",
            Self::EvalSha {
                read_only: true, ..
            } => "EVALSHA_RO",
            Self::EvalSha { .. } => "EVALSHA",
            Self::ScriptLoad(_)
            | Self::ScriptExists(_)
            | Self::ScriptFlush
            | Self::ScriptKill
            | Self::ScriptDebug
            | Self::ScriptHelp => "SCRIPT",
        }
    }

    pub(crate) fn may_write_data(&self) -> bool {
        match self {
            Self::Eval(eval) => !eval.read_only,
            Self::EvalSha { read_only, .. } => !read_only,
            Self::ScriptLoad(_)
            | Self::ScriptExists(_)
            | Self::ScriptFlush
            | Self::ScriptKill
            | Self::ScriptDebug
            | Self::ScriptHelp => false,
        }
    }
}
