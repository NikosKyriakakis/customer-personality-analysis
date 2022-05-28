initial_data = LOAD '../input.data/personality_analysis_clean.csv' USING PigStorage(';') AS (	
	ID:int,
	Age:int,
	Education:chararray,
	Marital_Status:chararray,
	Income:float,
	Kidhome:int,
	Teenhome:int,
	Dt_Customer:chararray,
	Recency:int,
	MntWines:int,
	MntFruits:int,
	MntMeatProducts:int,
	MntFishProducts:int,
	MntSweetProducts:int, 
	MntGoldProds:int,
	NumDealsPurchases:int,
	NumWebPurchases:int,
    NumCatalogPurchases:int,
	NumStorePurchases:int,
	NumWebVisitsMonth:int,
	AcceptedCmp3:int,
	AcceptedCmp4:int,
	AcceptedCmp5:int,
	AcceptedCmp1:int,
    AcceptedCmp2:int,	
	Complain:int,
	Response:int
	);



initial_data = FOREACH initial_data GENERATE *, MntWines + MntFruits + MntMeatProducts + MntFishProducts + MntSweetProducts + MntGoldProds AS total_spent:int, SUBSTRING (Dt_Customer,(LAST_INDEX_OF(Dt_Customer, '/')+1),(int)SIZE(Dt_Customer)) AS year:chararray;
grouped = GROUP initial_data all;

mean_spent = FOREACH grouped GENERATE AVG(initial_data.total_spent) AS average_spent;
total_spent_bounds = FOREACH mean_spent GENERATE *, 1.5*mean_spent.average_spent as upper_bound:float, 0.25*mean_spent.average_spent as lower_bound:float;
mean_income = FOREACH grouped GENERATE AVG(initial_data.Income) AS average_income;

Bronze = FILTER initial_data BY (year == '21') AND (Income < mean_income.average_income) AND (total_spent <= total_spent_bounds.lower_bound);
Bronze = FOREACH Bronze GENERATE $0 AS ID:int,'Bronze' AS category:chararray;
Bronze = ORDER Bronze BY ID DESC;

Bronze = FOREACH (GROUP Bronze BY category) GENERATE group,TOTUPLE(Bronze.ID);

Paper = FILTER initial_data BY (year != '21') AND (Income < mean_income.average_income) AND (total_spent <= total_spent_bounds.lower_bound);
Paper = FOREACH Paper GENERATE $0 AS ID:int,'Paper' AS category:chararray;
Paper = ORDER Paper BY ID DESC;
 
Paper = FOREACH (GROUP Paper BY category) GENERATE group,Paper.ID;


Gold = FILTER initial_data BY (year == '21') AND (Income > 69500.0) AND (total_spent > total_spent_bounds.upper_bound);
Gold = FOREACH Gold GENERATE $0 AS ID:int,'Gold' AS category:chararray;
Gold = ORDER Gold BY ID DESC;

Gold_to_final = Gold;

Silver = FILTER initial_data BY (year != '21') AND (Income > 69500.0) AND (total_spent > total_spent_bounds.upper_bound);
Silver = FOREACH Silver GENERATE $0 AS ID:int,'Silver' AS category:chararray;
Silver = ORDER Silver BY ID DESC;

Silver_to_final = Silver;

final = UNION Gold_to_final, Silver_to_final;

final_result = FOREACH (GROUP final BY category) GENERATE group as Class, FLATTEN(TOTUPLE(final.ID)) as id;

Gold = GROUP Gold BY $1;
Gold = FOREACH Gold GENERATE group,Gold.ID;

Silver = GROUP Silver BY $1;
Silver = FOREACH Silver GENERATE group,Silver.ID;


STORE final_result INTO 'my_third_output' USING PigStorage(',');
STORE Bronze INTO 'Bronze_category' USING PigStorage(',');
STORE Paper INTO 'Paper_category' USING PigStorage(',');