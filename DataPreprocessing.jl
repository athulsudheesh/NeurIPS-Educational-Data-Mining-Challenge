 
using CSVFiles 
using DataFrames 
using FreqTables # for contegency table 
using Dates

# C:\\PhD\\NeurIPS EDM Challenge\\NeurIPS challenge data\\ == path to the data 
# 
item_response = DataFrame(load("C:\\PhD\\NeurIPS EDM Challenge\\NeurIPS challenge data\\train_data\\train_task_1_2.csv"))
q_meta = DataFrame(load("C:\\PhD\\NeurIPS EDM Challenge\\NeurIPS challenge data\\metadata\\question_metadata_task_1_2.csv"))
q_annotation = DataFrame(load("C:\\PhD\\NeurIPS EDM Challenge\\NeurIPS challenge data\\metadata\\subject_metadata.csv"))
dob = DataFrame(load("C:\\PhD\\NeurIPS EDM Challenge\\NeurIPS challenge data\\metadata\\student_metadata_task_1_2.csv"))

dictionary = Dict()

# Creating the dictionary of subjects with their IDs, this makes is easier to replace the IDs with the subject names in many other places 
for elements in eachrow(q_annotation)
    dictionary[elements[:SubjectId]] =  elements[:Name]
end

# While reading the file, the CSVFiles reader was loading the column `:SubjectId` as string
# whereas it is actually a column of Arrays with integer value in it, 
# hence we need to convert them into Arrays
q_meta[:SubjectId] = map(x->parse.(Int64,x),[split(chop(elements, head=1), ",") for elements in q_meta[:SubjectId] ])

# Out of all the infromation in item response, we are only interrested in Student ID(UserId), QuestionId and IsCorrect 
select!(item_response,[:QuestionId,:UserId,:IsCorrect])

# Converting the long form data to wide format 
# unstack is a function in DataFrames package that helps us to achieve this. 
# Here QuestionId becomes the columns and :IsCorrect the values in each entry 
Xᵢⱼ = unstack(item_response,:QuestionId, :IsCorrect)

# q_meta is a collection of arrays with varying lenghts of elements in it 
# flatten function unzips these elements into new rows
# the new long form table will only be having single element (:SubjectId) in them 
Q_stacked = flatten(q_meta,:SubjectId)

# freqtable is a function for creating contigency table 
# we can use this function to create the Q matrix 
Q_table = freqtable(Q_stacked,:QuestionId,:SubjectId)

# Q_table is of type NamedArray, so converting them into DataFrame 
Q = DataFrame(Q_table)
names!(Q,Symbol.(names(Q_table)[2]))


# Replacing Missing values with 0000-01-0000 
dob[:DateOfBirth] = coalesce.(dob[:DateOfBirth], DateTime(0))

# From the dataofbirth column we only need the year of birth 
dob[:DateOfBirth] = Dates.Year.(dob[:DateOfBirth])

# We need to group the X matrices based on the birth year of the subjects 
Xwithdob = innerjoin(Xᵢⱼ,dob, on = :UserId)
Xdobgrouped = groupby(Xwithdob, :DateOfBirth)

Threads.@threads for i in 1:length(Xdobgrouped)
    filename = unique(Xdobgrouped[i][:DateOfBirth])[1]
    tmp = select(Xdobgrouped[i], Not([:Gender,:DateOfBirth, :PremiumPupil]))
    CSV.write("X_$filename.csv", tmp)
end

# Similarly grouping Q matrices by years to match the X matrice years 

item_response_withdob = innerjoin(item_response, dob, on=:UserId)
item_response_grouped = groupby(item_response_withdob, :DateOfBirth)
Q_byyear = [DataFrame()]

for i in 1:length(item_response_grouped)
    Q_table = freqtable(flatten(select(innerjoin(DataFrame(item_response_grouped[i]), q_meta, on=:QuestionId), [:QuestionId,:SubjectId]),:SubjectId),:QuestionId,:SubjectId)
    Q_df = DataFrame(Q_table)
    names!(Q_df,Symbol.(names(Q_table)[2]))
    filename = unique(item_response_grouped[i][:DateOfBirth])[1]
    CSV.write("Q_$filename.csv",Q_df)
end