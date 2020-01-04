# External Merge Sort

## Goal

 - _Exploits the parallelism of resources such as processors, storages_
 
 - _Minimize the interference among threads_

## Usage
 _1. Download the source files_
 > git clone https://github.com/khmyung/extsort.git

 _2. Compile the codes_
 > cd extsort
 > make
 
 _3. Check options_
 > ./extsort -h

 _4. Run the external sort_
 > (example) ./extsort -G -R -M -P -d 1 -m 128 -t 4
