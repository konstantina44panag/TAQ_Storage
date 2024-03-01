#!/usr/bin/env python3
import h5py

# Path to your HDF5 file
hdf5_file_path = '200701.h5'  

# Open the HDF5 file in read mode
with h5py.File(hdf5_file_path, 'r') as f:
    dataset_paths = ['03/MSFT/ctm/table', '03/IBM/ctm/table']

    for dataset_path in dataset_paths:
        if dataset_path in f:
            dataset = f[dataset_path]
            data = dataset[:] 
            print(f"Successfully accessed dataset at path: {dataset_path}")
        else:
            print(f"Dataset at path {dataset_path} not found")





