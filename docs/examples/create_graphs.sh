rm -rf .merkl
merkl dot --cache file --rankdir LR pipeline1.my_pipeline 3 | dot -Tpng -opipeline1.png
merkl run --cache file pipeline1.my_pipeline 3
merkl dot --cache file --rankdir LR pipeline1.my_pipeline 3 | dot -Tpng -opipeline2.png
merkl dot --cache file --rankdir LR pipeline2.my_pipeline 3 | dot -Tpng -opipeline3.png

merkl dot --cache file --rankdir LR pipeline3.train_eval | dot -Tpng -opipeline4.png
rm -rf .merkl
