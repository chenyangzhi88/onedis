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
