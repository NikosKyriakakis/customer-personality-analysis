data = LOAD '../input.data/personality_analysis_clean.csv' USING PigStorage(';');

data = RANK data;
data = FILTER data BY ($0 > 1);

groupedby = GROUP data BY $3;

count = FOREACH groupedby GENERATE group as education_status, COUNT(data) as cnt;

ordered_data = ORDER count BY education_status;

STORE ordered_data INTO 'my_first_output' USING PigStorage('\t');