#!/bin/bash

cwltool --print-dot workflows/malaria-amplicon-workflow.cwl | dot -Tpng  -o diagram3.png
cwltool --print-dot workflows/malaria-scatter-workflow.cwl | dot -Tpng  -o diagram2.png
cwltool --print-dot workflows/malaria-with-reports-clean.cwl | dot -Tpng  -o diagram1.png
