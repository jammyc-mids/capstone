#pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import os
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import torch
from torch.utils.data import Dataset
import torch.nn.functional as F

class ResBase(Dataset):
    def __init__(self, basedir: str, filenames: list):
        self.basedir = basedir
        self.filenames = filenames
        self.filepaths = [os.path.join(basedir, filename) for filename in filenames]

        self.metadatas = [pq.read_metadata(p) for p in self.filepaths]

        self.lengths = [metadata.num_rows - 1 for metadata in self.metadatas]
        self.lengths_cumsum = np.cumsum(self.lengths)

        self.col_names = [c.name for c in self.metadatas[0].schema]

        indicies = {}
        for i, col in enumerate(self.col_names):
            _id = '__'.join(col.split('__')[:2])

            if _id not in indicies:
                indicies[_id] = i
        self.signal_idx = indicies

        self.total_length = sum(self.lengths)

        self.current_file_idx = None
        self.current_table = {}

    def __len__(self):
        return self.total_length

    def __getitem__(self, idx):
        file_idx = np.searchsorted(self.lengths_cumsum, idx)

        if file_idx == 0:
            row_idx = idx
        else:
            row_idx = idx - self.lengths_cumsum[file_idx-1]

        if file_idx != self.current_file_idx:
            self.current_file_idx = file_idx
            self.current_table = (
                pd.read_parquet(self.filepaths[file_idx])
                .values
                .astype(np.float32)
            )

        row = torch.from_numpy(self.current_table[row_idx, :])

        return row

class AllSignals(ResBase):
    def __len__(self):
        return self.total_length

    def __getitem__(self, idx):
        row = super().__getitem__(idx)

        t = row[3:6] # time features

        h = torch.concat(
            (
                F.one_hot(row[6].long(), num_classes=49), #state onehot pylint: disable=not-callable
                F.one_hot(row[7].long(), num_classes=5), #bldg onehot pylint: disable=not-callable
                row[8:13], # rest of house features
            ),
            dim=0
        )

        x = row[13:109] # aggregate load
        T = row[109:205] # temperature

        y = row[205:].reshape(-1, 96) # individual load profiles (11, 96)

        return t, h, x, T, y

class SingleLoad(ResBase):
    def __init__(self, basedir: str, filenames: list, load_label: str):
        super().__init__(basedir, filenames)

        if not load_label.startswith('y__'):
            raise ValueError('load_label must start with "y__"')

        if not load_label in self.signal_idx:
            raise ValueError(f'load_label "{load_label}" not found in dataset')

        self.load_label = load_label
        self.load_idx = self.signal_idx[load_label]

    def __getitem__(self, idx):
        row = super().__getitem__(idx)

        x = row[13:13+96] # aggregate load
        y = row[self.load_idx: self.load_idx + 96] # individual load profile

        return x, y


