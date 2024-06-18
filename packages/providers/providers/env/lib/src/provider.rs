use std::env;

use nv_provider_core::{Provider, ProviderError};

#[derive(Debug, Default)]
pub struct EnvProvider {}

#[async_trait::async_trait]
impl Provider for EnvProvider {
    fn name(&self) -> &'static str {
        "env"
    }

    fn initialize(&self) {}
    fn destroy(&self) {}

    async fn get_value(&self, key: &str) -> Result<Vec<u8>, ProviderError> {
        log::debug!("Getting EnvProvider value for key {}", key);
        let result = env::var(key).map_err(|error| {
            log::error!("Env prov err {:?}", error);

            match error {
                env::VarError::NotPresent => ProviderError::NoValueForKey,
                _ => ProviderError::ExplodeyProvider,
            }
        });

        log::debug!("Got EnvProvider value for key {} = {:?}", key, result);

        result.map(|s| {
            let sli = &s[..];
            let by = sli.as_bytes();

            let ve: Vec<_> = by.into();

            ve
        })
    }
}
