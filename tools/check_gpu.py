"""Small utility to check CUDA/GPU availability for PyTorch and TensorFlow."""
import sys
import torch

def check_torch():
    print("PyTorch version:", torch.__version__)
    print("CUDA available:", torch.cuda.is_available())
    try:
        print("CUDA device count:", torch.cuda.device_count())
        for i in range(torch.cuda.device_count()):
            print(f" - Device {i}: {torch.cuda.get_device_name(i)}")
    except Exception as e:
        print("Error querying CUDA devices:", e)

def check_tensorflow():
    try:
        import tensorflow as tf
        print("TensorFlow version:", tf.__version__)
        gpus = tf.config.list_physical_devices('GPU')
        print("TensorFlow GPU devices:", gpus)
        if gpus:
            try:
                for g in gpus:
                    tf.config.experimental.set_memory_growth(g, True)
                print("Enabled memory growth on TensorFlow GPUs")
            except Exception as e:
                print("Could not set memory growth:", e)
    except Exception as e:
        print("TensorFlow not available or failed to import:", e)

def main():
    print("--- GPU Check ---")
    check_torch()
    check_tensorflow()

if __name__ == '__main__':
    main()
