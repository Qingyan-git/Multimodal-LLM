import torch

print(f'CUDA available: {torch.cuda.is_available()}\n')

print(f'Using GPU: {torch.cuda.get_device_name(0)}\n')