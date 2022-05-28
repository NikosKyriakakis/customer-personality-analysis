rm -rf ../cat_output

hadoop com.sun.tools.javac.Main CategoryDriver.java DistributiveExpenseMapper.java DistributiveIncomeMapper.java DistributiveMapper.java DistributiveReducer.java DistributiveWineMapper.java EducationDriver.java Main.java MeanBase.java MeanWritable.java PersonalityAnalysisConstants.java TupleWritable.java WineDriver.java
jar cf cli_cat.jar CategoryDriver.class 'CategoryDriver$CategoryMapper.class' 'CategoryDriver$CategoryReducer.class' MeanWritable.class DistributiveMapper.class DistributiveIncomeMapper.class DistributiveExpenseMapper.class DistributiveReducer.class Main.class PersonalityAnalysisConstants.class MeanBase.class 'MeanBase$MeanMapper.class' 'MeanBase$MeanReducer.class'
hadoop jar cli_cat.jar Main --cli-cat ../input_data ../income_partial ../mean_income ../expenses_partial ../mean_expenses ../cat_output

echo -e "\n=================== Client Category Output ====================="
cat ../cat_output/part-r-00000
echo "=============================================================="
rm -rf ../mean_income
rm -rf ../mean_expenses
rm -rf ../income_partial
rm -rf ../expenses_partial

rm 'CategoryDriver$CategoryMapper.class' 'CategoryDriver$CategoryReducer.class' CategoryDriver.class DistributiveExpenseMapper.class DistributiveIncomeMapper.class DistributiveMapper.class DistributiveReducer.class DistributiveWineMapper.class 'EducationDriver$EducationMapper.class' 'EducationDriver$EducationReducer.class' EducationDriver.class Main.class 'MeanBase$MeanMapper.class' 'MeanBase$MeanReducer.class' MeanBase.class MeanWritable.class PersonalityAnalysisConstants.class TupleWritable.class 'WineDriver$OrderMapper.class' 'WineDriver$OrderReducer.class' 'WineDriver$WineMapper.class' WineDriver.class
rm cli_cat.jar