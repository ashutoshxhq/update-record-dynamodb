use aws_sdk_dynamodb::{model::AttributeValue, types::DateTime, Client};
use egnitely_client::{Context, Error, HandlerError};
use serde::{Deserialize, Serialize};
use serde_dynamo::aws_sdk_dynamodb_0_17::to_item;
use serde_json::{json, Value};
use std::{collections::HashMap, time::SystemTime};

#[derive(Debug, Serialize, Deserialize)]
struct FunctionContextData {
    pub table_name: String,
    pub primary_key: String,
    pub index_data: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FunctionInput {
    pub filter: HashMap<String, String>,
    pub data: HashMap<String, Value>,
}

pub async fn handler(mut _ctx: Context, _input: FunctionInput) -> Result<Value, Error> {
    let config_data = serde_json::from_value::<FunctionContextData>(_ctx.config())?;

    let config = aws_config::from_env().region("ap-south-1").load().await;

    if let Some(_primary_key_value) = _input.filter.get(&config_data.primary_key) {
        let mut update_expression = "set ".to_string();
        let mut exp_attr_values: HashMap<String, Value> = HashMap::new();
        let mut exp_attr_names: HashMap<String, String> = HashMap::new();

        for (data_key, data_value) in _input.data.clone().into_iter() {
            update_expression.push_str(&format!("#{}_key = :{}_val, ", data_key.clone(), data_key));
            exp_attr_names.insert(format!("#{}_key", data_key.clone()), data_key.clone());
            exp_attr_values.insert(format!(":{}_val", data_key), data_value);
        }

        update_expression.push_str("#updated_at_key = :updated_at_val");
        exp_attr_names.insert("#updated_at_key".to_string(), "updated_at".to_string());

        let mut exp_attr_values_item = to_item(exp_attr_values.clone())?;
        exp_attr_values_item.insert(
            ":updated_at_val".to_string(),
            AttributeValue::N(DateTime::from(SystemTime::now()).as_nanos().to_string()),
        );

        let client = Client::new(&config);
        let _res = client
            .update_item()
            .table_name(config_data.table_name)
            .set_key(Some(to_item(_input.filter)?))
            .set_update_expression(Some(update_expression))
            .set_expression_attribute_names(Some(exp_attr_names))
            .set_expression_attribute_values(Some(exp_attr_values_item))
            .send()
            .await?;
        Ok(json!({
                "message": "Successfully updated record",
        }))
    } else {
        return Err(HandlerError::new(
            "INVALID_FILTER".to_string(),
            "Please provide table's primary key in filter".to_string(),
            400,
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trigger_function() {

        let resp = handler(
            Context::new(
                "test".to_string(),
                "test".to_string(),
                json!({
                    "table_name": "functions",
                    "primary_key": "id"
                }),
                json!({}),
            ),
            FunctionInput {
                filter: HashMap::from([("id".to_string(), "9b999589-e1eb-4356-a477-c38df2d3a681".to_string())]),
                data: HashMap::from([("name".to_string(), json!("delete_function"))]),
            },
        )
        .await
        .unwrap();

        assert_eq!("Successfully updated record", resp["message"]);
    }
}
