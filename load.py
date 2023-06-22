import pandas as pd
import mysql.connector
import parameters



host = parameters.param.host
db = parameters.param.db
user = parameters.param.user
passw = parameters.param.user

source_path = parameters.param.source_path

data = pd.read_csv(source_path)

conn = mysql.connector.connect(host=host, database=db, user=user, password=passw)
cursor = conn.cursor()

for _, row in data.iterrows():
    sql = "insert into aspen_monitor.lean_detail(ID, RAGStatus,	Location, How_was_the_problem_opportunity_identified,	Problem_Opportunity_Identified,	Project_Leader,	Owning_IT_Tower,	Participating_IT_Tower,	Capability,	TeamName,	Sponsor,	Criticality,	Team_members,	Facilitator_Lean_Champion_Lean_Coach,	ProjectType,	Attachments,	Start_Date,	End_Date,	Year_Accounted,	Status,	Comments,	Manager_name,	Approval_from_manager,	LeanProcess_Verification_Type,	Lean_Process_Verifier,	Approval_from_verifier,	Geography_Impacted,	WastesRemoved,	Benefits_of_your_project,	Impacted_category,	Lean_tools_being_used,	IT2025_Strategy_Pillar,	Hour_Savings_For_IT,	Hour_Savings_Business_Users,	Total_Hours_Saved,	Repurpose_IT_savings_efforts,	Financial_Savings_Others,	Created,	Modified_By,	Last_Modified_Date,	Created_By) VALUES (%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s)"
    values =(row['ID'],	row['RAG Status'],	row['Location'],	row['How was the problem / idea / opportunity identified?'],	row['Problem / Idea / Opportunity Identified'],	row['Project Leader'],	row['Owning IT Tower'],	row['Participating IT Tower'],	row['Capability'],	row['Team Name'],	row['Sponsor'],	row['Criticality'],	row['Team member(s)'],	row['Facilitator / Lean Champion / Lean Coach'],	row['Project Type'],	row['Attachments'],	row['Start Date'],	row['End Date'],	row['Year Accounted'],	row['Status'],	row['Comments'],	row['Manager name'],	row['Approval from manager?'],	row['Lean Process Verification Type'],	row['Lean Process Verifier'],	row['Approval from verifier'],	row['Geography Impacted'],	row['Wastes Removed'],	row['What is the impact / business value / benefits of your project?'],	row['Impacted category'],	row['Lean tools being used'],		row['IT2025 Strategy Pillar'],	row['Hour Savings For IT'],	row['Hour Savings for Business Users'],	row['Total Hours Saved'],	row['Repurpose IT savings efforts'],	row['Financial Savings or Others'],	row['Created'],	row['Modified By'],	row['Last Modified Date'],	row['Created By'])
    cursor.execute(sql,values)


conn.commit()
cursor.close()
conn.close()