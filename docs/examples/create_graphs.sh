rm -rf .merkl
merkl dot --cache file --rankdir LR test.my_pipeline 3 | dot -Tpng -opipeline1.png
merkl run --cache file test.my_pipeline 3
merkl dot --cache file --rankdir LR test.my_pipeline 3 | dot -Tpng -opipeline2.png
merkl dot --cache file --rankdir LR test2.my_pipeline 3 | dot -Tpng -opipeline3.png
rm -rf .merkl
