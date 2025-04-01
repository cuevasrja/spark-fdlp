import numpy as np
from typing import List
import random

# for reproducibility of the results
random.seed(42)

def sigmoid(x: np.ndarray) -> np.ndarray:
    """
    Applies the sigmoid function to the input array.

    Parameters:
        x (np.ndarray): Input array.

    Returns:
        np.ndarray: Output array after applying the sigmoid function.
    """
    return 1 / (1 + np.exp(-x))


def sigmoid_derivative(sx: np.ndarray) -> np.ndarray:
    """
    Compute the derivative of the sigmoid function.

    Parameters:
    sx (np.ndarray): Input array.

    Returns:
    np.ndarray: Derivative of the sigmoid function.
    """
    return sx * (1 - sx)


class MultiNeuralNetwork:
    """
    A multi-layer neural network implementation.

    Parameters:
    - num_inputs (int): The number of input features.
    - num_hidden (int): The number of neurons in the hidden layer.
    - num_outputs (int): The number of output neurons.
    - learning_rate (float, optional): The learning rate for weight updates. Default is 0.001.
    - num_epochs (int, optional): The maximum number of training epochs. Default is 10000.
    - tol (float, optional): The tolerance for convergence. Default is 1e-9.
    - name (str, optional): The name of the neural network. Default is "MultiLayerPerceptron".

    Attributes:
    - learning_rate (float): The learning rate for weight updates.
    - n_iters (int): The maximum number of training epochs.
    - tol (float): The tolerance for convergence.
    - weights_hidden (np.ndarray): The weights for the hidden layer.
    - weights_output (np.ndarray): The weights for the output layer.
    - bias_hidden (np.ndarray): The bias for the hidden layer.
    - bias_output (np.ndarray): The bias for the output layer.
    - output_hidden (np.ndarray): The output of the hidden layer.
    - output_out (np.ndarray): The output of the output layer.

    Methods:
    - forward(xi: np.ndarray) -> np.ndarray: Performs forward propagation through the neural network.
    - backward(xi: np.ndarray, error: np.ndarray) -> None: Performs backward propagation through the neural network.
    - fit(X: np.ndarray, y: np.ndarray) -> List[float]: Trains the neural network on the given input and target data.
    - test(X: np.ndarray) -> np.ndarray: Performs the testing phase of the neural network.
    """

    def __init__(
        self,
        num_inputs: int,
        num_hidden: int,
        num_outputs: int,
        learning_rate: float = 0.001,
        num_epochs: int = 10000,
        tol: float = 1e-9,
        name: str = "MultiLayerPerceptron",
        bias_hidden: np.ndarray = None,
        bias_output: np.ndarray = None,
        weights_hidden: np.ndarray = None,
        weights_output: np.ndarray = None,
    ):
        self.learning_rate = learning_rate
        self.n_iters = num_epochs
        self.tol = tol

        # Initialize all network weights to small random numbers (e.g., between -.05 and .05).
        # for the hidden and output layer
        self.weights_hidden = np.random.uniform(-0.05, 0.05, (num_hidden, num_inputs)) if weights_hidden is None else weights_hidden
        self.weights_output = np.random.uniform(-0.05, 0.05, (num_outputs, num_hidden)) if weights_output is None else weights_output
        self.bias_hidden = np.zeros(num_hidden) if bias_hidden is None else bias_hidden
        self.bias_output = np.zeros(num_outputs) if bias_output is None else bias_output
        self.name = name

    def forward(self, xi: np.ndarray) -> np.ndarray:
        """
        Performs forward propagation through the neural network.

        Args:
            xi (np.ndarray): Input data for a single sample.

        Returns:
            np.ndarray: Output of the neural network.
        """
        # z1 = w1 * x + b1 in the hidden layer
        z1 = np.dot(xi, self.weights_hidden.T) + self.bias_hidden
        # output of the hidden layer
        self.output_hidden = sigmoid(z1)

        # z2 = w2 * a1 + b2 in the output layer
        z2 = np.dot(self.output_hidden, self.weights_output.T) + self.bias_output
        # output of the output layer
        self.output_out = sigmoid(z2)

        return self.output_out

    def backward(self, xi: np.ndarray, error: np.ndarray) -> None:
        """
        Performs backward propagation through the neural network.

        Args:
            xi (np.ndarray): Input data for a single sample.
            error (np.ndarray): Error in the output layer.

        Returns:
            None
        """
        # Compute the gradient of the error with respect to the output layer
        derror = error * sigmoid_derivative(self.output_out)

        # Compute the gradient of the error with respect to the hidden layer
        dhidden = derror.dot(self.weights_output) * sigmoid_derivative(
            self.output_hidden
        )

        # Update the weights and biases
        self.bias_output += self.learning_rate * derror
        self.output_hidden = self.output_hidden.reshape(-1, 1)
        derror = derror.reshape(-1, 1)
        self.weights_output += self.learning_rate * self.output_hidden.dot(derror.T).T

        xi = xi.reshape(-1, 1)
        self.weights_hidden += self.learning_rate * (xi * dhidden).T
        dhidden = dhidden.reshape(-1)
        self.bias_hidden += self.learning_rate * dhidden.T

    def fit(self, X: np.ndarray, y: np.ndarray) -> List[float]:
        """
        Trains the neural network on the given input and target data.

        Parameters:
        - X (np.ndarray): The input data.
        - y (np.ndarray): The target outputs.

        Returns:
        - avg_error_history (list): The average error history during training.

        """
        avg_error_history = []

        # convert -1 to 0
        y = np.where(y == -1, 0, y)

        msg: str = f"Training {self.name} for {self.n_iters} epochs\n"
        for i in range(self.n_iters):
            avg_error = 0
            for xi, yi in zip(X, y):
                # forward pass
                output = self.forward(xi)

                target = yi
                if len(self.bias_output) > 1:
                    target = np.zeros(len(self.bias_output))
                    target[int(yi)] = 1

                error = target - output

                # backward pass
                self.backward(xi, error)

                # mse
                error = 0.5 * np.sum(error**2)
                # update the average error
                avg_error += error

            # update the average error history
            avg_error_history.append(avg_error / X.shape[0])

            if avg_error / X.shape[0] < self.tol:
                print(f"Converged at epoch {i} with error {avg_error / X.shape[0]}")
                break

        return avg_error_history

    def test(self, X: np.ndarray) -> np.ndarray:
        """
        Performs the testing phase of the neural network.

        Args:
            X (np.ndarray): The input data to be tested.

        Returns:
            np.ndarray: The predicted output for each input sample.
        """
        output = []
        for i in X:
            out = self.forward(i)
            if len(out) > 1:
                output.append(np.argmax(out))
            else:
                if out[0] >= 0.5:
                    output.append(1)
                else:
                    output.append(0)
        return np.array(output)
