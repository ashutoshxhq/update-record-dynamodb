use aws_sdk_dynamodb::{
    model::{AttributeValue},
    Client, types::DateTime,
};
use egnitely_client::{HandlerError, RequestContext, Result};
use serde::{Deserialize, Serialize};
use serde_dynamo::aws_sdk_dynamodb_0_17::{to_item};
use serde_json::{json, Value};
use std::{collections::HashMap, time::SystemTime};

#[derive(Debug, Serialize, Deserialize)]
struct FunctionContextData {
    pub table_name: String,
    pub primary_key: String,
    pub index_data: Option<HashMap<String, String>>,
    pub token_claims: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FunctionInput {
    pub filter: HashMap<String, String>,
    pub data: HashMap<String, Value>,
}

pub async fn handler(mut _ctx: RequestContext, _input: Value) -> Result<Value> {
    let context_data = serde_json::from_value::<FunctionContextData>(_ctx.data())?;
    let input_data = serde_json::from_value::<FunctionInput>(_input)?;

    if let Some(sdk_config) = _ctx.aws_sdk_config() {
        if let Some(_primary_key_value) = input_data.filter.get(&context_data.primary_key) {
            let mut update_expression = "set ".to_string();
            let mut exp_attr_values: HashMap<String, Value> = HashMap::new();
            let mut exp_attr_names: HashMap<String, String> = HashMap::new();

            for (data_key, data_value) in input_data.data.clone().into_iter() {
                update_expression.push_str(&format!("#{}_key = :{}_val, ", data_key.clone(), data_key));
				exp_attr_names.insert(format!("#{}_key", data_key.clone()), data_key.clone());
				exp_attr_values.insert(format!(":{}_val", data_key), data_value);
            }

            update_expression.push_str("#updated_at_key = :updated_at_val");
			exp_attr_names.insert("#updated_at_key".to_string(), "updated_at".to_string());

			let mut exp_attr_values_item = to_item(exp_attr_values.clone())?;
			exp_attr_values_item.insert(":updated_at_val".to_string(), AttributeValue::N(DateTime::from(SystemTime::now()).as_nanos().to_string()));

			println!("Condition: {}", update_expression);
			println!("Names: {:?}", exp_attr_names);
			println!("Values: {:?}", exp_attr_values);

            let client = Client::new(&sdk_config);
            let _res = client
                .update_item()
                .table_name(context_data.table_name)
                .set_key(Some(to_item(input_data.filter)?))
                .set_update_expression(Some(update_expression))
				.set_expression_attribute_names(Some(exp_attr_names))
				.set_expression_attribute_values(Some(exp_attr_values_item))
                .send()
                .await?;
            Ok(json!({
                    "message": "Successfully updated record",
            }))
        } else {
            return Err(Box::new(HandlerError::new(
                "INVALID_FILTER".to_string(),
                "Please provide table's primary key in filter".to_string(),
            )));
        }
    } else {
        return Err(Box::new(HandlerError::new(
            "NO_SDK_CONFIG".to_string(),
            "No aws sdk config found in handler context".to_string(),
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::Credentials;

    #[tokio::test]
    async fn trigger_function() {
        let config = aws_config::from_env()
            .credentials_provider(Credentials::new(
                "PUT_ACCESS_KEY_HERE".to_string(),
                "PUT_ACCESS_SECRET_HERE".to_string(),
                None,
                None,
                "local",
            ))
            .region("ap-south-1")
            .load()
            .await;

        let resp = handler(
            RequestContext::new(
                "test".to_string(),
                "test".to_string(),
                Some(config),
                json!({
                    "table_name": "functions",
					"primary_key": "id"
                }),
                json!({}),
            ),
            json!({
				"filter":{
					"id":"9b999589-e1eb-4356-a477-c38df2d3a681"
				},
				"data":{
					"name": "update_function",
				}
			}),
        )
        .await
        .unwrap();

        assert_eq!("Successfully updated record", resp["message"]);
    }
}
