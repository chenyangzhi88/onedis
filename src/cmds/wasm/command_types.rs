pub enum WasmCommand {
    Load {
        name: String,
        bytes: Vec<u8>,
    },
    Call {
        name: String,
        function: String,
        args: Vec<String>,
        read_only: bool,
    },
    Scan {
        name: String,
        function: String,
        prefix: String,
        limit: usize,
    },
    Delete {
        name: String,
    },
    FunctionLoad {
        name: String,
        bytes: Vec<u8>,
    },
    FunctionDelete {
        name: String,
    },
    FunctionList,
    List,
}
