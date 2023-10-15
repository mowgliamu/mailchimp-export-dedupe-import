# Exporting Segments / Dedupllication / Creating and Writing to New Audiences

# 1. Preparation for Exporting Segments: Getting segments info and preparing a run file

Input: A csv file containing the name and id of all segments to be exported (usually called `segments.csv`) 

Run the following command:

`python3  prepare_input_export.py > all_segments_info &!`

Output files:

	- `all_segments_info`: Contains total number of members for each segment along with name/id (ascending order)
	- `run_export_segments`: The file which has commands to run the export segments for individual segments

Make the run file executable:

`chmod +x run_export_segment`


# 2. Exporting Segments

Run the follwing command and go get some coffee! This will take some time:

`./run_export_segment > output_export_segments &!`

Ideally, this will export all the segments wihtout any problems. But things are never ideal. Inevitably, some
segments will fail to export (mainly because of the annoying TIMEOUT issue). Check which ones have failed.
The ones which have been completed can be quickly grepped through the otput file:

`grep -A 1 SUCCESS output_export_segments`

Or one can simply do a manual check as well (can be automated). 

Modify the `run_export_segment` to keep only the failed ones. Play with COUNT and OFFSET, if needed.

Re-run:

`./run_export_segment > output_export_segments &!`

### NOTE: This problem can be avoided with BATCH run. However, I could not get BATCH to run successfully for exporting segments. 
### By default, it only exports 10 members, and the count and offset parameters just do not work here. This is never a problem
### in the audience batch export, for example. I also tried Node.js, so it's not a python issue, but mot likely a bug in API. 

Anyways, re-run the export command until all the segments have been exported.
All the segment files will be present in the newly created directory (in Step 1) called `All_segments`.

# 3. Deduplication

This is fairly straightforward. Run the following command:

`python3 deduplication.py > output_deduplication &!`

This will create the deduplicated segments in a directory called `All_segments_deduplicated`.

# 4. Creating new audiences

In this step, we are creating the (empty) new audiences to which the deduplicated data will  be written. 
This step can be run independently, for example, while waiting for the segment export to be finished.

Input: A csv file containing the id and names of the audiences (usually called `audience.csv`)

Set `CREATE_NEW_AUDIENCE = True` in `write_audience_members.py` and run:

`python3 write_audience_members.py audience.csv > output_create_audience &!`

# 5. Writing new audience members 

Once done with deduplication and audience creation, set `WRITE_AUDIENCE_MEMBERS = True` and `CREATE_NEW_AUDIENCE = False` and run:

`python3 write_audience_members.py audience.csv &!`

This will interactively ask for each audience whether to proceed with writing the members:

Type Yes/Y/y if you are sure about writing the new members. 


# TODO

- Use logging (for logs - info/warnings/errors)
- Raise exceptions
- Unit tests


