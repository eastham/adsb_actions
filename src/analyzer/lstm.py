import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import train_test_split
import logging
import time

# --- Logging Setup ---
#logging.basicConfig(
#    level=logging.INFO,
#    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
#    handlers=[logging.StreamHandler()]
#)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Suppress matplotlib debug/info logs
logging.getLogger("matplotlib").setLevel(logging.WARNING)

import matplotlib.pyplot as plt

# Set a random seed for reproducibility
torch.manual_seed(42)
np.random.seed(42)

# --- 1. Data Generation (Simulated ADS-B Data) ---

global_min = np.array([40.717537, -119.320302, 4000.0])
global_max = np.array([40.832509, -119.0744069, 8000.0])
ref_points = [
    (40.783388, -119.232725, "P1-25"),
    (40.807354, -119.216621, "P2-25"),
    (40.803107, -119.181667, "P3-25"),
    (40.776557, -119.176181, "P4-25"),
    (40.764363, -119.207719, "P5-25"),
]

def euclidean_loss_xy(pred, target):
    # pred and target shape: (batch, seq_len, features)
    # Only use the first two columns (x=lat, y=long)
    diff = pred[..., :2] - target[..., :2]
    dist = torch.sqrt(torch.sum(diff ** 2, dim=-1))
    return dist.mean()

def generate_synthetic_data(num_aircraft=5, points_per_aircraft=300):
    data = []
    for i in range(num_aircraft):
        tail_number = f'N{i:03d}AB'
        # Start within global min/max
        start_lat = np.random.uniform(global_min[0], global_max[0])
        start_long = np.random.uniform(global_min[1], global_max[1])
        start_alt = np.random.uniform(global_min[2], global_max[2])

        # Trends chosen so that the track stays within bounds
        max_lat_trend = (global_max[0] - global_min[0]) / points_per_aircraft
        max_long_trend = (global_max[1] - global_min[1]) / points_per_aircraft
        max_alt_trend = (global_max[2] - global_min[2]) / points_per_aircraft

        lat_trend = np.random.uniform(-max_lat_trend, max_lat_trend)
        long_trend = np.random.uniform(-max_long_trend, max_long_trend)
        alt_trend = np.random.uniform(-max_alt_trend, max_alt_trend)

        lat, long, alt = start_lat, start_long, start_alt

        for t in range(points_per_aircraft):
            timestamp = pd.to_datetime('2025-01-01 00:00:00') + pd.Timedelta(seconds=t)
            # Add small noise
            lat += lat_trend + np.random.normal(0, 0.0001)
            long += long_trend + np.random.normal(0, 0.0001)
            alt += alt_trend + np.random.normal(0, 1)
            # Clip to global min/max
            lat = np.clip(lat, global_min[0], global_max[0])
            long = np.clip(long, global_min[1], global_max[1])
            alt = np.clip(alt, global_min[2], global_max[2])
            data.append([timestamp, tail_number, lat, long, alt])

        # Optionally, re-sort in case of any drift
    df = pd.DataFrame(data, columns=['timestamp', 'tail_number', 'lat', 'long', 'alt'])
    df = df.sort_values(by=['tail_number', 'timestamp']).reset_index(drop=True)
    return df

class FixedMinMaxScaler:
    def __init__(self, min_vals, max_vals):
        self.min = np.array(min_vals)
        self.max = np.array(max_vals)
        self.scale = self.max - self.min

    def transform(self, X):
        # Clip values to the global min/max before scaling
        X_clipped = np.clip(X, self.min, self.max)
        if np.any(X != X_clipped):
            n_clipped = np.sum(np.any(X != X_clipped, axis=1))
            logger.warning("Warning: %d rows had values outside global min/max and were clipped.", n_clipped)
        return (X_clipped - self.min) / self.scale

    def inverse_transform(self, X_scaled):
        X_unscaled = X_scaled * self.scale + self.min
        # Optionally, clip again to ensure within bounds
        X_unscaled_clipped = np.clip(X_unscaled, self.min, self.max)
        if np.any(X_unscaled != X_unscaled_clipped):
            n_clipped = np.sum(np.any(X_unscaled != X_unscaled_clipped, axis=1))
            logger.warning("Warning: %d rows were clipped to global min/max during inverse_transform.", n_clipped)
        return X_unscaled_clipped

def create_sequences(data, n_steps_in, n_steps_out):
    X, Y = [], []
    for i in range(len(data)):
        end_ix = i + n_steps_in
        out_end_ix = end_ix + n_steps_out
        if out_end_ix > len(data):
            break
        seq_x, seq_y = data[i:end_ix], data[end_ix:out_end_ix]
        X.append(seq_x)
        Y.append(seq_y)
    return np.array(X), np.array(Y)

class Encoder(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers):
        super(Encoder, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(input_size, hidden_size,
                            num_layers, batch_first=True, dropout=0.2)

    def forward(self, x):
        outputs, (hidden, cell) = self.lstm(x)
        return hidden, cell

class Decoder(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(Decoder, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.output_size = output_size
        self.lstm = nn.LSTM(input_size, hidden_size,
                            num_layers, batch_first=True, dropout=0.2)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x, hidden, cell):
        outputs, (hidden, cell) = self.lstm(x, (hidden, cell))
        predictions = self.fc(outputs)
        return predictions

class Seq2Seq(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, n_steps_out, output_size):
        super(Seq2Seq, self).__init__()
        self.encoder = Encoder(input_size, hidden_size, num_layers)
        self.decoder = Decoder(input_size, hidden_size, num_layers, output_size)
        self.n_steps_out = n_steps_out

    def forward(self, input_seq, target_seq=None, teacher_forcing_ratio=0.5):
        batch_size = input_seq.size(0)
        device = input_seq.device
        outputs = torch.zeros(batch_size, self.n_steps_out, self.decoder.output_size, device=device)

        encoder_hidden, encoder_cell = self.encoder(input_seq)
        decoder_input = input_seq[:, -1, :].unsqueeze(1)
        hidden, cell = encoder_hidden, encoder_cell

        for t in range(self.n_steps_out):
            out = self.decoder(decoder_input, hidden, cell)
            outputs[:, t:t+1, :] = out
            if self.training and target_seq is not None and np.random.rand() < teacher_forcing_ratio:
                decoder_input = target_seq[:, t:t+1, :]
            else:
                decoder_input = out
        return outputs

def run_lstm_pipeline(
    df=None,
    num_aircraft=10,
    points_per_aircraft=500,
    n_steps_in=180,
    n_steps_out=30,
    hidden_size=32,
    num_layers=1,
    batch_size=64,
    num_epochs=6,
    plot=True,
    loadfile=None
):
    logger.info("Starting LSTM pipeline...")

    # Data generation or use provided DataFrame
    if df is None:
        logger.info("Generating synthetic data...")
        df = generate_synthetic_data(num_aircraft=num_aircraft, points_per_aircraft=points_per_aircraft)
        logger.info("Synthetic data generated.")
    else:
        logger.info("Using provided DataFrame.")

    logger.info("Sample Data Head:\n%s", df.head(100))
    logger.info("Data Info:\n%s", df.info())

    logger.info("Beginning data preprocessing...")
    features = ['lat', 'long', 'alt']
    n_features = len(features)

    scaler = FixedMinMaxScaler(global_min, global_max)

    all_X, all_y = [], []
    seq_tail_numbers = []
    processed_ctr = 0
    not_processed_ctr = 0

    for tail_number in df['tail_number'].unique():
        aircraft_df = df[df['tail_number'] == tail_number].copy()
        # Ensure correct dtypes
        for col in features:
            if not np.issubdtype(aircraft_df[col].dtype, np.number):
                raise TypeError(f"Column {col} must be numeric, got {aircraft_df[col].dtype}")
            if aircraft_df[col].isnull().any():
                raise ValueError(f"Column {col} contains NaN values.")
        aircraft_data = aircraft_df[features].values.astype(np.float32)
        scaled_data = scaler.transform(aircraft_data)
        logger.info("Scaled data for tail number %s: %s", tail_number, scaled_data[:5])
        X_aircraft, y_aircraft = create_sequences(
            scaled_data, n_steps_in, n_steps_out)
        if X_aircraft.shape[0] > 0:
            all_X.append(X_aircraft)
            all_y.append(y_aircraft)
            seq_tail_numbers.extend([tail_number] * X_aircraft.shape[0])
            logger.info("Processed tail number %s: %d sequences created.", tail_number, X_aircraft.shape[0])
            processed_ctr += 1
        else:
            not_processed_ctr += 1
            logger.warning("No valid sequences found for tail number %s. Skipping.", tail_number)

    logger.info("Successful aircraft traces processed: %d", processed_ctr)
    logger.info("Aircraft traces with no valid sequences: %d", not_processed_ctr)
    if not all_X or not all_y:
        logger.error("No valid sequences found in the data. Check input DataFrame.")
        return None
    X_np = np.concatenate(all_X, axis=0)
    y_np = np.concatenate(all_y, axis=0)
    seq_tail_numbers = np.array(seq_tail_numbers)

    logger.info("Shape of X_np (input sequences): %s", X_np.shape)
    logger.info("Shape of y_np (output sequences): %s", y_np.shape)
    logger.info("Shape of seq_tail_numbers: %s", seq_tail_numbers.shape)

    X_tensor = torch.tensor(X_np, dtype=torch.float32)
    y_tensor = torch.tensor(y_np, dtype=torch.float32)

    X_train, X_test, y_train, y_test, tail_train, tail_test = train_test_split(
        X_tensor, y_tensor, seq_tail_numbers, test_size=0.2, random_state=42)

    logger.info("Shape of X_train: %s", X_train.shape)
    logger.info("Shape of y_train: %s", y_train.shape)
    logger.info("Shape of X_test: %s", X_test.shape)
    logger.info("Shape of y_test: %s", y_test.shape)
    logger.info("Shape of tail_test: %s", tail_test.shape)

    train_dataset = TensorDataset(X_train, y_train)
    test_dataset = TensorDataset(X_test, y_test)

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

    logger.info("Data preprocessing complete.")
    logger.info("Building LSTM Encoder-Decoder model...")

    model = Seq2Seq(n_features, hidden_size, num_layers, n_steps_out, n_features)
    logger.info("Model Architecture:\n%s", model)
    logger.info("Model built.")

    if torch.backends.mps.is_available(): # and False: # XXX
        device = torch.device("mps")
        logger.info("Using Apple Silicon MPS backend for acceleration.")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
        logger.info("Using CUDA GPU for acceleration.")
    else:
        device = torch.device("cpu")
        logger.info("Using CPU.")

    model.to(device)

    criterion = euclidean_loss_xy
    #criterion = nn.MSELoss()  # Use MSELoss for simplicity, can be replaced with custom loss

    optimizer = optim.Adam(model.parameters(), lr=0.001)

    logger.info("Beginning model training...")
    train_losses = []
    val_losses = []

    if loadfile:
        logger.info("Loading model weights from %s", loadfile)
        model.load_state_dict(torch.load(loadfile, map_location=device))
        logger.info("Model weights loaded.")
    else:
        for epoch in range(num_epochs):
            epoch_start_time = time.time()

            model.train()
            running_loss = 0.0
            for batch_idx, (inputs, targets) in enumerate(train_loader):
                inputs, targets = inputs.to(device), targets.to(device)
                optimizer.zero_grad()
                outputs = model(inputs, targets, teacher_forcing_ratio=0.5)
                loss = criterion(outputs, targets)
                loss.backward()
                optimizer.step()
                running_loss += loss.item()

            avg_train_loss = running_loss / len(train_loader)
            train_losses.append(avg_train_loss)

            model.eval()
            val_loss = 0.0
            with torch.no_grad():
                for inputs, targets in test_loader:
                    inputs, targets = inputs.to(device), targets.to(device)
                    outputs = model(inputs)
                    loss = criterion(outputs, targets)
                    val_loss += loss.item()
            avg_val_loss = val_loss / len(test_loader)
            val_losses.append(avg_val_loss)

            epoch_end_time = time.time()
            if (epoch + 1) % 1 == 0:
                logger.info(
                'Epoch [%d/%d], Train Loss: %.4f, Val Loss: %.4f, Time: %.2f seconds',
                    epoch+1, num_epochs, avg_train_loss, avg_val_loss, 
                    epoch_end_time - epoch_start_time
                )
            logger.info("Epoch %d complete. Train Loss: %.4f, Val Loss: %.4f", epoch + 1, avg_train_loss, avg_val_loss)

    logger.info("Model training complete.")
    torch.save(model.state_dict(), 'model_weights.pth')

    logger.info("Evaluating model on test set...")
    model.eval()
    test_loss = 0.0
    with torch.no_grad():
        for inputs, targets in test_loader:
            inputs, targets = inputs.to(device), targets.to(device)
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            test_loss += loss.item()
    final_test_loss = test_loss / len(test_loader)
    logger.info("Final Test Loss (MSE): %.4f", final_test_loss)
    logger.info("Model evaluation complete.")

    logger.info("Making predictions on a test sample...")
    sample_index = 0
    input_sequence_tensor = X_test[sample_index].unsqueeze(0).to(device)
    true_output_sequence_tensor = y_test[sample_index].unsqueeze(0).to(device)
    sample_tail_number = tail_test[sample_index]

    with torch.no_grad():
        predicted_sequence_scaled_tensor = model(input_sequence_tensor, teacher_forcing_ratio=0.0)

    input_sequence_np = input_sequence_tensor.squeeze(0).cpu().numpy()
    true_output_sequence_np = true_output_sequence_tensor.squeeze(0).cpu().numpy()
    predicted_sequence_scaled_np = predicted_sequence_scaled_tensor.squeeze(0).cpu().numpy()

    original_input_sequence = scaler.inverse_transform(input_sequence_np)
    original_true_output_sequence = scaler.inverse_transform(true_output_sequence_np)
    original_predicted_sequence = scaler.inverse_transform(predicted_sequence_scaled_np)

    logger.info("Original Input Sequence (last 5 points):\n%s", original_input_sequence[-5:])
    logger.info("Original True Output Sequence:\n%s", original_true_output_sequence)
    logger.info("Predicted Output Sequence:\n%s", original_predicted_sequence)
    logger.info("Prediction complete.")

    if plot:
        logger.info("Visualizing results for 5 test samples...")
        num_samples = min(20, X_test.shape[0])
        for sample_index in range(num_samples):
            input_sequence_tensor = X_test[sample_index].unsqueeze(0).to(device)
            true_output_sequence_tensor = y_test[sample_index].unsqueeze(0).to(device)
            sample_tail_number = tail_test[sample_index]
            # Find the timestamp of the last input point for labeling
            # Find the corresponding row in df for this sequence
            # Get the indices in the original df for this test sample
            # Since we lose track of the original indices after train_test_split, we can only approximate
            # by using the sorted df for the given tail number and sequence index
            aircraft_df = df[df['tail_number'] == sample_tail_number].sort_values('timestamp')
            # The sequence index in the aircraft's data is ambiguous after shuffling, so we just get the last timestamp in the input sequence
            input_sequence_np = input_sequence_tensor.squeeze(0).cpu().numpy()
            true_output_sequence_np = true_output_sequence_tensor.squeeze(0).cpu().numpy()
            with torch.no_grad():
                predicted_sequence_scaled_tensor = model(input_sequence_tensor, teacher_forcing_ratio=0.0)
            predicted_sequence_scaled_np = predicted_sequence_scaled_tensor.squeeze(0).cpu().numpy()

            original_input_sequence = scaler.inverse_transform(input_sequence_np)
            original_true_output_sequence = scaler.inverse_transform(true_output_sequence_np)
            original_predicted_sequence = scaler.inverse_transform(predicted_sequence_scaled_np)

            # Try to get the timestamp for the last input point
            # Find all rows for this tail number, get the timestamps, and pick the one at the right offset
            timestamps = aircraft_df['timestamp'].values
            if len(timestamps) >= n_steps_in:
                last_input_timestamp = timestamps[-(len(timestamps) - n_steps_in)]
            else:
                last_input_timestamp = timestamps[-1] if len(timestamps) > 0 else "unknown"

            full_true_trajectory = np.concatenate(
                (original_input_sequence, original_true_output_sequence), axis=0)
            full_predicted_trajectory = np.concatenate(
                (original_input_sequence, original_predicted_sequence), axis=0)

            plt.figure(figsize=(12, 8))
            plt.plot(full_true_trajectory[:, 1], full_true_trajectory[:, 0], 'b-o', label='True Trajectory (Lat/Long)')
            plt.plot(full_predicted_trajectory[n_steps_in:, 1], full_predicted_trajectory[n_steps_in:, 0], 'r--x', label='Predicted Trajectory (Lat/Long)')
            plt.plot(original_input_sequence[:, 1], original_input_sequence[:, 0], 'g-s', label='Input History (Lat/Long)')
            plt.title(f'Aircraft Trajectory Prediction (Lat/Long) - PyTorch\nTail: {sample_tail_number}, Last Input Timestamp: {last_input_timestamp}')
            plt.xlabel('Longitude')
            plt.ylabel('Latitude')
            plt.legend()
            plt.grid(True)
            # Set fixed axis limits based on global_min and global_max
            plt.xlim(global_min[1], global_max[1])
            plt.ylim(global_min[0], global_max[0])
            # plot some reference points

            for lat, lon, _ in ref_points:
                plt.plot(lon, lat, 'ko-')  # draw lines between the points
                ref_lats = [lat for lat, lon, _ in ref_points]
                ref_lons = [lon for lat, lon, _ in ref_points]
                plt.plot(ref_lons, ref_lats, 'k-', linewidth=1)
            plt.show()
            if (False):     # alt graph
                plt.figure(figsize=(12, 8))
                time_steps_input = np.arange(n_steps_in)
                time_steps_output = np.arange(n_steps_in, n_steps_in + n_steps_out)

                plt.plot(time_steps_input, original_input_sequence[:, 2], 'g-s', label='Input History (Altitude)')
                plt.plot(time_steps_output, original_true_output_sequence[:, 2], 'b-o', label='True Future Altitude')
                plt.plot(time_steps_output, original_predicted_sequence[:, 2], 'r--x', label='Predicted Future Altitude')
                plt.title(f'Aircraft Altitude Prediction - PyTorch\nTail: {sample_tail_number}, Last Input Timestamp: {last_input_timestamp}')
                plt.xlabel('Time Step (seconds)')
                plt.ylabel('Altitude')
                plt.legend()
                plt.grid(True)
                plt.show()
        logger.info("Visualization complete.")

    return {
        "model": model,
        "scaler": scaler,
        "test_loss": final_test_loss,
        "input_sequence": original_input_sequence,
        "true_output_sequence": original_true_output_sequence,
        "predicted_output_sequence": original_predicted_sequence,
        "df": df
    }

class LSTMPipeline:
    """
    LSTM pipeline class that maintains a DataFrame context and provides a callback
    for adding aircraft positions, as well as a method to run the LSTM analysis.
    """

    def __init__(self, df=None):
        self.skip_oob = 0
        self.skip_illegal = 0
        self._pending_rows = []
        if df is not None:
            self.df = df
        else:
            self.df = pd.DataFrame(columns=['timestamp', 'tail_number', 'lat', 'long', 'alt'])

    def add_aircraft_position(self, lat, long, alt, timestamp, tail_number):
        """
        Add a single aircraft position to the internal dataframe.
        Aborts with an error if types are inappropriate.
        """
        # Type checks
        if not isinstance(lat, (float, int)):
            raise TypeError(f"lat must be float or int, got {type(lat)}")
        if not isinstance(long, (float, int)):
            raise TypeError(f"long must be float or int, got {type(long)}")
        if not isinstance(alt, (float, int)):
            raise TypeError(f"alt must be float or int, got {type(alt)}")
        if not isinstance(tail_number, str):
            raise TypeError(f"tail_number must be str, got {type(tail_number)}")
        # Accept int/float (epoch seconds), str, or pd.Timestamp for timestamp
        if isinstance(timestamp, (int, float)):
            ts = pd.to_datetime(timestamp, unit='s')
        elif isinstance(timestamp, str) or isinstance(timestamp, pd.Timestamp):
            ts = pd.to_datetime(timestamp)
        else:
            raise TypeError(f"timestamp must be int, float, str, or pd.Timestamp, got {type(timestamp)}")

        # Optionally, check for NaN
        if any(pd.isna(val) for val in [lat, long, alt, tail_number, ts]):
            raise ValueError("None or NaN value detected in aircraft position input.")

        # skip point if not within global min/max
        if not (global_min[0] <= lat <= global_max[0] and
                global_min[1] <= long <= global_max[1] and
                global_min[2] <= alt <= global_max[2]):
            #if "FEMG_2" in tail_number:
            #    logger.debug("Skipping out-of-bounds point: tail=%s, lat=%.6f, long=%.6f, alt=%.2f", tail_number, lat, long, alt)
            self.skip_oob += 1
            return
        # skip tail_numbers starting with "N10"
        if tail_number.startswith("N10"):
            self.skip_illegal += 1
            return
        
        new_row = {
            'timestamp': ts,
            'tail_number': tail_number,
            'lat': float(lat),
            'long': float(long),
            'alt': float(alt)
        }
        self._pending_rows.append(new_row)

    def finalize(self):
        """Call this after all add_aircraft_position calls to build the DataFrame."""
        if self._pending_rows:
            new_df = pd.DataFrame(self._pending_rows)
            self.df = pd.concat([self.df, new_df], ignore_index=True)
            self._pending_rows = []

    def run(self, **kwargs):
        """
        Run the LSTM pipeline using the current dataframe.
        Additional keyword arguments are passed to run_lstm_pipeline.
        """
        self.finalize()
        logger.warning("skipctr skipped oob: %d illegal: %d", self.skip_oob, self.skip_illegal)
        self.df = self.df.sort_values(
            by=['tail_number', 'timestamp']).reset_index(drop=True)
        logger.warning("dataframe dimensions: %s", self.df.shape)
        #loadfile = "model_weights.pth"
        loadfile = None
        return run_lstm_pipeline(df=self.df, loadfile=loadfile, **kwargs)

# The rest of your code (run_lstm_pipeline, model classes, etc.) remains unchanged.

# Replace the old add_aircraft_position function with the class-based approach above.

if __name__ == "__main__":
    pipeline = LSTMPipeline()
    # Example usage: pipeline.add_aircraft_position(...)
    pipeline.run()
