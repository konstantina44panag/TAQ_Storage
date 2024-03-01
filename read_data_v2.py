'''Reading performance of Structure2 '''
#To Read datasets = ['03/MSFT/ctm/table', '04/MSFT/ctm/table']

#!/usr/bin/env python3
import h5py

hdf5_file_path = '200701_v2.h5'  

with h5py.File(hdf5_file_path, 'r') as f:
    dataset_paths = ['MSFT/03/ctm/table', 'MSFT/04/ctm/table']

    for dataset_path in dataset_paths:
        if dataset_path in f:
            dataset = f[dataset_path]
            data = dataset[:] 
            print(f"Successfully accessed dataset at path: {dataset_path}")
        else:
            print(f"Dataset at path {dataset_path} not found")
