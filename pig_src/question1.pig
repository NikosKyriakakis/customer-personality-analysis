data = LOAD '../input_data/personality_analysis_clean.csv' USING PigStorage(';');

groupedby = GROUP data BY $2;

count = FOREACH groupedby GENERATE group as education_status, COUNT(data) as cnt;

ordered_data = ORDER count BY education_status;

STORE ordered_data INTO 'my_first_output' USING PigStorage('\t');

dump ordered_data;
