rm -rf .dvc .git

git init
dvc init

merkl dot --cache dvc --rankdir LR pipeline1.my_pipeline 3 | dot -Tpng -opipeline1.png
merkl run --cache dvc pipeline1.my_pipeline 3
merkl dot --cache dvc --rankdir LR pipeline1.my_pipeline 3 | dot -Tpng -opipeline2.png
merkl dot --cache dvc --rankdir LR pipeline2.my_pipeline 3 | dot -Tpng -opipeline3.png

dvc add train.csv test.csv
merkl dot --cache dvc --rankdir LR pipeline3.train_eval | dot -Tpng -opipeline4.png

rm -rf .dvc .git .gitignore .dvcignore train.csv.dvc test.csv.dvc
