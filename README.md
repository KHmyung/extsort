# External Merge Sort

## Goal

 - _Exploits the data parallelism through range partitioning_
 
 - _Minimize the interference among internal resources of storage_

## Usage

 _1. Download the source files_
  > git clone https://github.com/khmyung/extsort.git

 _2. Compile the codes_
  > cd extsort

  > make
 
 _3. Set options shown with the below command_
  > ./extsort -h

 _4. Create directory layout_
  > ./mkdir.sh : input number means the number of worker threads

 _5. Run the external sort_
  > ./extsort -G -R -M -P -w 4	: example

