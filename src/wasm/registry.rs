use super::*;

pub struct WasmRegistry {
    engine: Engine,
    modules: DashMap<String, Module>,
    fuel_per_call: u64,
    max_memory_bytes: usize,
}

impl WasmRegistry {
    pub fn new() -> Self {
        let mut config = Config::default();
        config.consume_fuel(true);
        let engine = Engine::new(&config).expect("failed to create wasm engine");
        let fuel_per_call = std::env::var("ONEDIS_WASM_FUEL")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_WASM_FUEL);
        let max_memory_bytes = std::env::var("ONEDIS_WASM_MAX_MEMORY_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_WASM_MAX_MEMORY_BYTES);
        Self {
            engine,
            modules: DashMap::new(),
            fuel_per_call,
            max_memory_bytes,
        }
    }

    pub fn load(&self, name: &str, bytes: &[u8]) -> Result<()> {
        validate_name(name)?;
        if bytes.len() > MAX_WASM_MODULE_BYTES {
            return Err(Error::msg("ERR wasm module is too large"));
        }
        let module = Module::new(&self.engine, bytes)
            .map_err(|error| Error::msg(format!("ERR wasm compile failed: {error}")))?;
        validate_imports(&module)?;
        self.modules.insert(name.to_string(), module);
        Ok(())
    }

    pub fn delete(&self, name: &str) -> bool {
        self.modules.remove(name).is_some()
    }

    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .modules
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        names.sort();
        names
    }

    pub async fn call(
        &self,
        db: Arc<Db>,
        name: &str,
        function: &str,
        args: &[String],
        read_only: bool,
    ) -> Result<Vec<WasmValue>> {
        let module = self
            .modules
            .get(name)
            .ok_or_else(|| Error::msg("ERR wasm module not found"))?
            .clone();
        let mut store = Store::new(
            &self.engine,
            WasmHostContext {
                db,
                read_only,
                host_error: false,
                limits: WasmLimits::new(self.max_memory_bytes),
            },
        );
        store.limiter(|context| &mut context.limits);
        store.set_fuel(self.fuel_per_call)?;
        let linker = host_linker(&self.engine)?;
        let instance = linker
            .instantiate_async(&mut store, &module)
            .await
            .map_err(|error| Error::msg(format!("ERR wasm instantiate failed: {error}")))?;
        let func = instance
            .get_func(&mut store, function)
            .ok_or_else(|| Error::msg("ERR wasm function not found"))?;
        let func_type = func.ty(&store);
        let params = func_type.params().collect::<Vec<_>>();
        let results = func_type.results();
        let inputs = prepare_call_inputs(&mut store, &instance, &params, args)?;
        let mut outputs = results
            .map(|ty| {
                Val::default_for_ty(&ty)
                    .ok_or_else(|| Error::msg("ERR wasm result type is not supported"))
            })
            .collect::<Result<Vec<_>>>()?;
        func.call_async(&mut store, &inputs, &mut outputs)
            .await
            .map_err(|error| Error::msg(format!("ERR wasm call failed: {error}")))?;
        if store.data().host_error {
            return Err(Error::msg("ERR wasm host function failed"));
        }
        outputs.into_iter().map(WasmValue::from_val).collect()
    }

    pub async fn scan(
        &self,
        db: Arc<Db>,
        name: &str,
        function: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let module = self
            .modules
            .get(name)
            .ok_or_else(|| Error::msg("ERR wasm module not found"))?
            .clone();
        let mut store = Store::new(
            &self.engine,
            WasmHostContext {
                db: db.clone(),
                read_only: true,
                host_error: false,
                limits: WasmLimits::new(self.max_memory_bytes),
            },
        );
        store.limiter(|context| &mut context.limits);
        store.set_fuel(self.fuel_per_call)?;
        let linker = host_linker(&self.engine)?;
        let instance = linker
            .instantiate_async(&mut store, &module)
            .await
            .map_err(|error| Error::msg(format!("ERR wasm instantiate failed: {error}")))?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| Error::msg("ERR wasm module must export memory for scan"))?;
        let func = instance
            .get_func(&mut store, function)
            .ok_or_else(|| Error::msg("ERR wasm function not found"))?;
        let func_type = func.ty(&store);
        let params = func_type.params().collect::<Vec<_>>();
        let results = func_type.results().collect::<Vec<_>>();
        if !matches!(
            params.as_slice(),
            [ValType::I32, ValType::I32, ValType::I32, ValType::I32]
        ) || !matches!(results.as_slice(), [ValType::I32])
        {
            return Err(Error::msg(
                "ERR wasm scan function must be (i32,i32,i32,i32)->i32",
            ));
        }

        let rows = db.scan_string_prefix_async(prefix, limit).await;
        let mut matched = Vec::new();
        for (key, value) in rows {
            if key.len() > WASM_SCAN_MAX_FIELD_BYTES || value.len() > WASM_SCAN_MAX_FIELD_BYTES {
                continue;
            }
            memory
                .write(&mut store, WASM_SCAN_KEY_OFFSET, key.as_bytes())
                .map_err(|_| Error::msg("ERR wasm scan key does not fit in memory"))?;
            memory
                .write(&mut store, WASM_SCAN_VALUE_OFFSET, &value)
                .map_err(|_| Error::msg("ERR wasm scan value does not fit in memory"))?;
            let inputs = [
                Val::I32(WASM_SCAN_KEY_OFFSET as i32),
                Val::I32(key.len() as i32),
                Val::I32(WASM_SCAN_VALUE_OFFSET as i32),
                Val::I32(value.len() as i32),
            ];
            let mut outputs = [Val::I32(0)];
            func.call_async(&mut store, &inputs, &mut outputs)
                .await
                .map_err(|error| Error::msg(format!("ERR wasm scan call failed: {error}")))?;
            if matches!(outputs[0], Val::I32(value) if value != 0) {
                matched.push(key);
            }
        }
        Ok(matched)
    }
}

impl Default for WasmRegistry {
    fn default() -> Self {
        Self::new()
    }
}
