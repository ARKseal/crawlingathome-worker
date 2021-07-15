import multiprocessing as mp

def f(x, y):
    return x+y

if __name__ == '__main__':
    with mp.Pool(16) as pool:
        print( pool.starmap(f, [[i, i+1] for i in range(16)]) )