import socket
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

def trigger_slack_alert(context):
    webhook_token = BaseHook.get_connection('slack').password
    environment = 'Production' if socket.gethostname() == 'vm-airflow-prod' else 'Staging'
    slack_message= 'Task failure'
    slack_blocks = [
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "Task Failure"
			},
			"accessory": {
				"type": "button",
				"text": {
					"type": "plain_text",
					"text": "View Error Details"
				},
				"style": "danger",
				"url": context.get('task_instance').log_url
			}
		},
		{
			"type": "divider"
		},
		{
			"type": "section",
            "text": {
				"type": "mrkdwn",
                "text": f"*Task*: {context.get('task_instance').task_id}\n*DAG*: {context.get('task_instance').dag_id}\n*Execution time*: {context.get('execution_date')}\n*Environment*: {environment}"
			}
		},
		{
			"type": "divider"
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*Responsible*"
			},
			"accessory": {
				"type": "users_select",
				"placeholder": {
					"type": "plain_text",
					"text": "Select a user"
				},
				"action_id": "users_select-action"
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*Status*"
			},
			"accessory": {
				"type": "static_select",
				"placeholder": {
					"type": "plain_text",
					"text": "Set status"
				},
				"options": [
					{
						"text": {
							"type": "plain_text",
							"text": ":exclamation:Unresolved"
						},
						"value": "unresolved-value"
					},
					{
						"text": {
							"type": "plain_text",
							"text": ":white_check_mark: Resolved"
						},
						"value": "resolved-value"
					}
				],
				"action_id": "status_select-action"
			}
		},
		{
			"type": "divider"
		}
	]
    slack_failure_alert = SlackWebhookOperator(
        task_id='slack_failure_alert',
        http_conn_id='slack',
        webhook_token=webhook_token,
        message=slack_message,
        blocks=slack_blocks,
        username='Airflow'
    )
    return slack_failure_alert.execute(context=context)