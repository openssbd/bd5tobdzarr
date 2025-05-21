import sys

if len(sys.argv) != 3:
    print("Usage: python bd52zarr.py input_file.h5 output_file.zarr")
    sys.exit(1)

import ome_zarr
import h5py
import pandas as pd
import numpy as np
import anndata as ad
from ome_zarr.io import parse_url
from scipy.sparse import csr_matrix
import zarr
from ngff_tables_prototype.writer import write_table_points

file = sys.argv[1]
f = h5py.File(file, "r")
groups = f['data']

if __name__ == '__main__':
    count = 0
    objnum = 0
    while (str(count) in groups.keys()):
        name = "/data/" + str(count) + "/object/0"
        dset = f[name]
        objnum = objnum + dset.len()

        count = count + 1
    print(objnum)

    n_obs, n_vars = objnum, 4
    dfa = pd.DataFrame([], columns = ['t', 'z', 'y', 'x'])
    obs_raw = pd.DataFrame([], columns = ['ID', 'entity', 'radius', 'label', 'value'])
    
    count = 0
    ncount = 0
    while (str(count) in groups.keys()):
        name = "/data/" + str(count) + "/object/0"
        feat = "/data/" + str(count) + "/feature/0"
        dset = f[name]
        fset = f[feat]
        for i in range(dset.len()):
            addRow = pd.DataFrame([dset[i, 't'], dset[i, 'z'], dset[i, 'y'], dset[i, 'x']], index=dfa.columns).T
            ftRow = pd.DataFrame([dset[i, 'ID'].decode('utf8'), dset[i, 'entity'].decode('utf8'), dset[i, 'radius'].astype('str'), dset[i, 'label'].decode('utf8'), fset[i, 'value'].astype('str')], index=obs_raw.columns).T
            dfa = pd.concat([dfa, pd.DataFrame(addRow)], ignore_index=True)
            obs_raw = pd.concat([obs_raw, pd.DataFrame(ftRow)], ignore_index=True)
        count = count + 1
    
    col = obs_raw.loc[:, 'ID']
    col.transpose
    obsp_raw = pd.DataFrame(np.zeros((n_obs, n_obs)), index = col.astype(str), columns = col.astype(str))

    track = "/data/trackInfo"
    tset = f[track]
    for j in range(tset.len()):
        obsp_raw.loc[tset[j, 'from'].decode('utf8'), tset[j, 'to'].decode('utf8')] = 1

    adata = ad.AnnData(X = dfa, obs = obs_raw)

    adata.obsp["tracking"] = csr_matrix(obsp_raw)
    store = parse_url(sys.argv[2], mode="w").store
    root = zarr.group(store=store)

    tables_group = root.create_group(name="tables")
    write_table_points(
        group=tables_group,
        adata=adata
    )

if __name__ == '__main__':
    count = 0
    objnum = 0
    while (str(count) in groups.keys()):
        name = "/data/" + str(count) + "/object/0"
        dset = f[name]
#        print(dset.len())
        objnum = objnum + dset.len()
#        if (count < 5):
#            for i in range(dset.len()):
#                print(dset[i, 'radius'])
        count = count + 1
    print(objnum)

    n_obs, n_vars = objnum, 4
    dfa = pd.DataFrame([], columns = ['t', 'z', 'y', 'x'])
    obs_raw = pd.DataFrame([], columns = ['ID', 'entity', 'radius', 'label', 'value'])
    
    count = 0
    ncount = 0
    while (str(count) in groups.keys()):
        name = "/data/" + str(count) + "/object/0"
        feat = "/data/" + str(count) + "/feature/0"
        dset = f[name]
        fset = f[feat]
        for i in range(dset.len()):
            addRow = pd.DataFrame([dset[i, 't'], dset[i, 'z'], dset[i, 'y'], dset[i, 'x']], index=dfa.columns).T
            ftRow = pd.DataFrame([dset[i, 'ID'].decode('utf8'), dset[i, 'entity'].decode('utf8'), dset[i, 'radius'], dset[i, 'label'].decode('utf8'), fset[i, 'value']], index=obs_raw.columns).T
            dfa = pd.concat([dfa, pd.DataFrame(addRow)], ignore_index=True)
            obs_raw = pd.concat([obs_raw, pd.DataFrame(ftRow)], ignore_index=True)
        count = count + 1
