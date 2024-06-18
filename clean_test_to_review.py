from statistics import mean

def clean_data(sensors):
    readings = sorted(sensors)
    differences = [abs(readings[i] - readings[i + 1]) for i in range(len(readings) - 1)]
    outlier_list = [readings[i] for i in range(len(readings) - 1) if abs(readings[i] - readings[i + 1]) > 2]

    if all(diff < 2.0 for diff in differences):
        return mean(readings), "Reliable"
    elif (abs(readings[0] - readings[1]) > 2 and not abs(readings[3] - readings[2]) > 2) \
        or ((abs(readings[3] - readings[2]) > 2 and not abs(readings[0] - readings[1]) > 2)) :
        readings = [reading for reading in readings if reading not in outlier_list]
        return mean(list(readings)), "UnreliableSensorReading"
    elif (abs(readings[0] - readings[1]) > 2 and abs(readings[3] - readings[2]) > 2) or (abs(readings[2] - readings[1])):
        return None, "UnreliableRow"