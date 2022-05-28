rm -rf ../wine_output

hadoop com.sun.tools.javac.Main CategoryDriver.java DistributiveExpenseMapper.java DistributiveIncomeMapper.java DistributiveMapper.java DistributiveReducer.java DistributiveWineMapper.java EducationDriver.java Main.java MeanBase.java MeanWritable.java PersonalityAnalysisConstants.java TupleWritable.java WineDriver.java
jar cf wine.jar WineDriver.class 'WineDriver$WineMapper.class' 'WineDriver$OrderMapper.class' 'WineDriver$OrderReducer.class' MeanWritable.class DistributiveMapper.class DistributiveWineMapper.class DistributiveReducer.class Main.class PersonalityAnalysisConstants.class MeanBase.class 'MeanBase$MeanMapper.class' 'MeanBase$MeanReducer.class' TupleWritable.class
hadoop jar wine.jar Main --mnt-wine ../input_data ../partial_sums_counts ../mean_output ../unordered_output ../wine_output

echo -e "\n=================== Wine Expenses Output ====================="
cat ../wine_output/part-r-00000
echo "=============================================================="

rm -rf ../mean_output
rm -rf ../partial_sums_counts
rm -rf ../unordered_output

rm 'CategoryDriver$CategoryMapper.class' 'CategoryDriver$CategoryReducer.class' CategoryDriver.class DistributiveExpenseMapper.class DistributiveIncomeMapper.class DistributiveMapper.class DistributiveReducer.class DistributiveWineMapper.class 'EducationDriver$EducationMapper.class' 'EducationDriver$EducationReducer.class' EducationDriver.class Main.class 'MeanBase$MeanMapper.class' 'MeanBase$MeanReducer.class' MeanBase.class MeanWritable.class PersonalityAnalysisConstants.class TupleWritable.class 'WineDriver$OrderMapper.class' 'WineDriver$OrderReducer.class' 'WineDriver$WineMapper.class' WineDriver.class
rm wine.jar
