data = LOAD '../input.data/personality_analysis_clean.csv' USING PigStorage(';');

targeted_features = foreach data generate (chararray) $0 as ID, 
				(int) $1 as Age,
				(chararray) $2 as Education,
				(chararray) $3 as Marital_Status,
				(float) $4 as Income,
				(int) $9 as MntWines;

grouped =  GROUP targeted_features all;

mean_wine_spent = FOREACH grouped GENERATE AVG(targeted_features.MntWines) AS average_wine_spent;
mean_wine_spent = FOREACH mean_wine_spent GENERATE *, 1.5*mean_wine_spent.average_wine_spent as upper_bound:float;

ordered = ORDER targeted_features BY MntWines DESC, Income DESC;

ordered = FILTER ordered BY ($5 > mean_wine_spent.upper_bound);

rank = RANK ordered BY MntWines DESC, Income DESC, ID DESC;

STORE rank INTO 'my_second_output' USING PigStorage('\t');

dump rank;