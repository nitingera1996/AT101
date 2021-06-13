def convert_percentage_to_polarity_range(percentage):
    """ Converts b/w -1 to 1 """
    return (percentage - 50) / 50.0
