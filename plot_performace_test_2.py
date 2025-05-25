import pickle
import matplotlib.pyplot as plt
def plot_performance(database, cik):
    with open('database.pkl', 'rb') as handle:
        database = pickle.load(handle)

    data = database[cik]['overall_performances']
    return data


