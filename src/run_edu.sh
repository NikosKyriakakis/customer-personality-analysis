rm -rf ../edu_output
hadoop com.sun.tools.javac.Main CategoryDriver.java DistributiveExpenseMapper.java DistributiveIncomeMapper.java DistributiveMapper.java DistributiveReducer.java DistributiveWineMapper.java EducationDriver.java Main.java MeanBase.java MeanWritable.java PersonalityAnalysisConstants.java TupleWritable.java WineDriver.java
jar cf edu.jar EducationDriver.class 'EducationDriver$EducationMapper.class' 'EducationDriver$EducationReducer.class' Main.class PersonalityAnalysisConstants.class
hadoop jar edu.jar Main --edu-level ../input_data ../edu_output
echo -e "\n=================== Education Level Output ==================="
cat ../edu_output/part-r-00000
echo "=============================================================="
rm 'CategoryDriver$CategoryMapper.class' 'CategoryDriver$CategoryReducer.class' CategoryDriver.class DistributiveExpenseMapper.class DistributiveIncomeMapper.class DistributiveMapper.class DistributiveReducer.class DistributiveWineMapper.class 'EducationDriver$EducationMapper.class' 'EducationDriver$EducationReducer.class' EducationDriver.class Main.class 'MeanBase$MeanMapper.class' 'MeanBase$MeanReducer.class' MeanBase.class MeanWritable.class PersonalityAnalysisConstants.class TupleWritable.class WineDriver.class 'WineDriver$WineMapper.class' 'WineDriver$OrderReducer.class' 'WineDriver$OrderMapper.class'