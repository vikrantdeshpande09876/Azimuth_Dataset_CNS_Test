# Merging Azimuth datasets into consolidated source-to-target node structure and generating a graph in Cytoscape


```
Note: I've currently skipped out the fetal_development.csv from this problem statement since it has many NULL CL-IDs.
```


## a. Step 1:

### Read the raw csv of Azimuth data, and bring it in a preliminary usable structure (inspired from the structure used for the ASCT-B CCF reporter visualization tool)

<ol>
For preprocessing the excels, we will:
<li>Remove the first 8 lines</li>
<li>Drop the *COUNT* columns</li>
<li>Add a new column containing File-name for backtracking purposes</li>
<li>Register a new Pyspark-SQL view</li>
</ol>


## b. Step 2:

### Merge the cleaned dataframes.

<ol>
Next, in order to merge the dataframes into a source node -> target node with attributes for both nodes and the edge, we will:
<li>Manipulate each of the pyspark-sql views to bring them in the Source->Target network structure</li>
<li>For now I've hardcoded the logic for each dataframe. However, given more time I can make the code more robust to dynamic number of layers in each csv.</li>
<li>Perform union of each Pyspark-SQL view and write the final dataframe as tab-delimited. Cytoscape was erroring out while parsing & uploading a comma-separated final file.</li>
</ol>


## c. Step 3:

### Generate the graph.

<ol>
<li>Finally Cytoscape upload the file in Cytoscape, and rename the columns:</li>

AS/1,  AS/1/LABEL,  AS/1/ID,  AS/2,  AS/2/LABEL,  AS/2/ID,  LAYER_CONNECTION,  FILE

as

LABEL,  EXPANDED_LABEL,  AS/1/ID,  LABEL,  EXPANDED_LABEL,  AS/2/ID, LAYER_CONNECTION,  FILE

<li>Mark the AS/1/ID as Source-node, and AS/2/ID as Target-node.</li>
<li>Mark LAYER_CONNECTION, and FILE as edge-attributes.</li>
<li>We will use these to determine what file/organ this edge was derived from, and between which 2 layers was the source node -> target node mapping got from.</li>
</ol>


![Initial_Submission_Hierarchical_Graph](/Testing_Data_structures_for_cytoscape/Initial_Submission_Hierarchical_Graph.png?raw=True)


### A more accurate image of the visualization is available at:
[Initial_Submission_Hierarchical_Graph.pdf](/Testing_Data_structures_for_cytoscape/Initial_Submission_Hierarchical_Graph.pdf)




### PERSONAL REFERENCE NOTES:

```
NIH: National Institute of Health

HuBMAP: Human BioMolecular Atlas Program research that focuses on mapping a Human Body at a single cellular level, using High-Res Spatial Tissue Maps.
		The Atlas would contain function and relationships among the cells of the body.
Goal is to have a healthy human map, to compare against as a reference, when trying to identify disease.
200 expert scientist in 50 institutions
TTD and RTI: Transformative Technology Development and Rapid Technology Implementation
TMC: Tissue Mapping Center
HIVE: HuBMAP Integration, Visualization and Engagement (Mapping center responsible for building a vocabulary/reference to describe all of the data generated in HuBMAP)
Tissue collection -> Assays/analysis -> Data compilation -> Map generation -> Data Storage -> Dissemination/access of data

Seurat: Software package in R to analyze single-cell datasets: cell-clusters, which genes/sequences each cell contains, etc.
Used for cell annotation in HuBMAP (neuron, skin-cell, subset of T-Cell, subset of macrophages, etc.)


BIOMIC: BioMolecular Multimodal Imaging Center at Vanderbilt University is building a platform for imaging and molecular analysis, to create 3-D molecular atlases of human tissues
through high-res spatial maps. They use mass-spectrometry, microscopy, etc. for analyses of kidney tissues.
Autofluorescence Microscopy -> MALDI imaging mass-spectrometry on a specific section of tissue -> 
Autofluorescence Microscopy is for image registration and co-registering multiple sections of same tissue sample
Stained Microscopy is to study histological (microscopic) morphology (shape and texture)


ASCT+B:,  Anatomical Structure, Cell Types, and Biomarkers

ASCT+B tables aim to capture partonomy of ASCT+B stuff (part-whole hierarchical relationship eg- lipids, proteins, genes, or metabolic markers) to identify cell-types.
For developing a reference atlas of the healthy human adult body within HuBMAP, we need a unified view of AS, CT, and B. In close collaboration with other consortia, 
we agreed to focus on part_of relationships so that the network graph can be simplified to a hierarchy. 
This makes it easier to develop user interfaces that enable investigators to quickly drill down, in an intuitive way, from whole-body, to organ, to organ parts, to cell types, 
and eventually to specific biomarkers associated with those cells.


CCF:,  Common Coordinator Framework is a Portal on HuBMAP for visualizing reports displaying partonomy of Anatomical-Structures <-> Cell Types <-> Biomarkers.

Human cell Atlas might show maps of cells per tissue-type or anatomical structure.

Azimuth:,  App for reference-based single cell analysis

Azimuth Maps:
Functional background: ,  6 organs <-> 449 cell types
Dataset background: ,  1500 datasets, 50.2 Million cells uploaded and mapped
```