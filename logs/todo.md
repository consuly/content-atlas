When generating workflow we get this error on some request:

Response:
I encountered an issue processing your request. Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'messages.0.content.1: unexpected `tool_use_id` found in `tool_result` blocks: toolu_01U6TFSigXwaBMG3xL3NSzC7. Each `tool_result` block must have a corresponding `tool_use` block in the previous message.'}, 'request_id': 'req_011CVi74TFfyAMDAZDwFSdph'}
Step 4: Top Performing Platforms
success
rows
1.99s
Response:
I encountered an issue processing your request. Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'messages.0.content.1: unexpected `tool_use_id` found in `tool_result` blocks: toolu_01XqVdD55AFUcN7uAgX2ymYA. Each `tool_result` block must have a corresponding `tool_use` block in the previous message.'}, 'request_id': 'req_011CVi74bG8gAyh8tjUKoP8D'}
Step 5: Platform Performance Comparison
success
rows
12.33s
Response:
I encountered an issue processing your request. Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'messages.0.content.1: unexpected `tool_use_id` found in `tool_result` blocks: toolu_01HxRNXXyaFroNPMJYyFrWDu. Each `tool_result` block must have a corresponding `tool_use` block in the previous message.'}, 'request_id': 'req_011CVi75WZPGdu4RbrPQ6L46'}


For the Workflow, we want the user to be able to remove, add steps and variables


For the Workflow, when processing an analysis we want to be able to ask for the LLM to generate one report with all the information mapped inside the report.

For the Workflow, we want to log the execution so we can go back to it

Keep only the primary email and personal email. Create only one column email in the new table. If the row has two different emails address create a new entry per unique email with the second email duplicate the information. Map only two columns phone number, one for the contact personal phone number and one for the company phone number. Standardize and internationalize the phone number with the + international code at the begining. Create a regex to clean-up all the phone number and output them in the same format. For the other columns only map the one already existing in the table.


We want to include Anthropic Claude as an option for the LLM that can be define in the .env. We want to be able to specify the provider (Claude or Gemini) and the model (claude-sonnet-4-5 or gemini-2.5-pro)

Map only those columns from the file:
Contact Full Name,First Name,Last Name,Title,Department,Seniority,Company Name,Website,List,Primary Email,Contact LI Profile URL,Personal Email,Contact Phone,Company Phone,Company Street 1,Company Street 2,Company City,Company State,Company Post Code,Company County,Company Annual Revenue,Company Description,Company Website Domain,Company Founded Date,Company Industry,Company LI Profile Url,Company LinkedIn ID,Company Revenue Range,Company Staff Count,Company Staff Count Range
Keep only the primary email and personal email. Create only one column email in the new table. If the row has two different emails address create a new entry per unique email with the second email duplicate the information. Map only two columns phone number, one for the contact personal phone number and one for the company phone number. Standardize and internationalize the phone number with the + international code at the begining.


Map the file using the existing columns from the table without adding new columns
Use only one column email in the new table. If the row has two different emails address create a new entry per unique email with the second email duplicate the information. Map only two columns phone number, one for the contact personal phone number and one for the company phone number. Standardize and internationalize the phone number with the + international code at the begining.


Standardize and internationalize the phone number by prefixing it with the + international code. Create a regex to clean up all the phone numbers and output them in the same format.