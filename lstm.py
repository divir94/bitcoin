import random
import math
import tensorflow as tf
from collections import namedtuple
import pickle
import time
import numpy as np

SAVE_PATH = "save_dir"

config_nt = namedtuple("Config",
                       ["batch_size",
                        "num_steps",
                        "num_features",
                        "num_targets",
                        "lstm_size",
                        "num_lstm_layers",
                        "hidden_size",
                        "max_grad_norm",
                        "init_scale",
                        "lr_decay",
                        "learning_rate"])

default_config = config_nt(batch_size=10,
                           num_steps=100,
                           num_features=10,
                           num_targets=2,
                           lstm_size=100,
                           num_lstm_layers=3,
                           hidden_size=100,
                           max_grad_norm=5,
                           init_scale=0.05,
                           lr_decay=0.8,
                           learning_rate=1.0)

test_config = config_nt(batch_size=2,
                        num_steps=2,
                        num_features=10,
                        num_targets=2,
                        lstm_size=2,
                        num_lstm_layers=3,
                        hidden_size=2,
                        max_grad_norm=5,
                        init_scale=0.05,
                        lr_decay=0.8,
                        learning_rate=1.0)


def lstm_cell(config):
    return tf.contrib.rnn.BasicLSTMCell(config.lstm_size)


class Model(object):
    def __init__(self, experiment_name, config, is_training):
        self.name = experiment_name
        self.config = config
        self.input = tf.placeholder(tf.float32,
                                    [config.batch_size, config.num_steps, config.num_features])
        self.target = tf.placeholder(tf.float32,
                                     [config.batch_size, config.num_steps, config.num_targets])
        cells = [lstm_cell(config) for _ in range(config.num_lstm_layers)]
        stacked_lstm = tf.contrib.rnn.MultiRNNCell(cells)
        preprocessing_w = tf.get_variable("preprocessing_w",
                                          [config.num_features, config.hidden_size],
                                          dtype=tf.float32)
        preprocessing_b = tf.get_variable("preprocessing_b",
                                          [config.hidden_size],
                                          dtype=tf.float32)
        reshaped_input = tf.reshape(self.input, [config.num_steps * config.batch_size, config.num_features])
        unshaped_preprocessed = tf.nn.relu(tf.matmul(reshaped_input, preprocessing_w) + preprocessing_b)
        preprocessed = tf.reshape(unshaped_preprocessed,
                                  [config.batch_size, config.num_steps, config.hidden_size])
        lstm_w = tf.get_variable("lstm_w",
                                 [config.lstm_size, config.num_targets],
                                 dtype=tf.float32)
        lstm_b = tf.get_variable("lstm_b",
                                 [config.num_targets],
                                 dtype=tf.float32)
        predictions = []
        self.loss = 0.0
        # Initial state of the LSTM memory.
        self.initial_state = stacked_lstm.zero_state(config.batch_size, tf.float32)
        state = self.initial_state
        print("preprocessed shape", preprocessed.get_shape())
        with tf.variable_scope("RNN"):
            print("initializing RNN")
            for time_step in range(config.num_steps):
                # The value of state is updated after processing each batch of words.
                if time_step > 0:
                    tf.get_variable_scope().reuse_variables()
                output, state = stacked_lstm(preprocessed[:, time_step, :], state)
                # The LSTM output can be used to make next state predictions
                prediction = tf.matmul(output, lstm_w) + lstm_b
                predictions.append(prediction)
                self.loss += tf.reduce_mean(tf.abs(prediction - self.target[:, time_step, :]))
            print("Done initializing RNN")
        self.final_state = state
        if not is_training:
            return

        self.lr = tf.Variable(0.0, trainable=False)
        tvars = tf.trainable_variables()
        grads, _ = tf.clip_by_global_norm(tf.gradients(self.loss, tvars),
                                          config.max_grad_norm)
        optimizer = tf.train.GradientDescentOptimizer(self.lr)
        self.train_op = optimizer.apply_gradients(
            zip(grads, tvars),
            global_step=tf.contrib.framework.get_or_create_global_step())

        self._new_lr = tf.placeholder(tf.float32, shape=[], name="new_learning_rate")
        self._lr_update = tf.assign(self.lr, self._new_lr)

    def assign_lr(self, session, lr_value):
        session.run(self._lr_update, feed_dict={self._new_lr: lr_value})


class DataGenerator(object):
    def __init__(self, path, batch_size, num_steps):
        self.batch_size = batch_size
        self.num_steps = num_steps
        with open(path, "rb") as f:
            self.xes, self.yes = pickle.load(f)
        self.reset()

    def generate_next_batch(self):
        if self._step >= self.max_steps:
            self.reset()
        batch_xes = []
        batch_yes = []
        for batch_num in range(self.batch_size):
            index = batch_num * len(self.xes) / self.batch_size + self._step * self.num_steps + self._offset
            batch_xes.append(self.xes[index: index + self.num_steps])
            batch_yes.append(self.yes[index: index + self.num_steps])

    def reset(self):
        self._step = 0
        self._offset = random.randint(0, self.num_steps - 1)
        self.max_steps = int(math.floor((len(self.xes) - self._offset) / float(self.batch_size * self.num_steps)))


def run_epoch(session, model, data_generator, eval_op=None, verbose=True):
    """Runs the model on the given data."""
    start_time = time.time()
    costs = 0.0
    state = session.run(model.initial_state)
    fetches = {
        "loss": model.loss,
        "final_state": model.final_state,
    }
    if eval_op is not None:
        fetches["eval_op"] = eval_op

    data_generator.reset()
    max_steps = data_generator.max_steps
    for step in range(max_steps):
        batch_xes, batch_yes = data_generator.generate_next_batch()
        feed_dict = {model.input: batch_xes, model.target: batch_yes}
        for i, (c, h) in enumerate(model.initial_state):
            feed_dict[c] = state[i].c
            feed_dict[h] = state[i].h
        values = session.run(fetches, feed_dict)
        cost = values["loss"]
        state = values["final_state"]
        costs += cost

        if verbose:
            print("cost", cost)
    print("time for epoch", (time.time() - start_time) / 60.0)
    return costs / max_steps


def main():
    config = default_config
    with tf.Graph().as_default():
        initializer = tf.random_uniform_initializer(-config.init_scale, config.init_scale)

        with tf.name_scope("Train"):
            with tf.variable_scope("Model", reuse=None, initializer=initializer):
                m = Model(experiment_name="trial", is_training=True, config=config)
            tf.summary.scalar("Training Loss", m.loss)
            tf.summary.scalar("Learning Rate", m.lr)

        train_data = DataGenerator("features_and_targets.pickle", config.batch_size, config.num_steps)
        sv = tf.train.Supervisor(logdir=SAVE_PATH)
        with sv.managed_session() as session:
            for i in range(100):
                lr_decay = config.lr_decay ** max(i - 10, 0.0)
                m.assign_lr(session, config.learning_rate * lr_decay)

                print("Epoch: %d Learning rate: %.3f" % (i + 1, session.run(m.lr)))
                train_loss = run_epoch(session, m, train_data, eval_op=m.train_op, verbose=True)
                print("Epoch: %d Train Loss: %.3f" % (i + 1, train_loss))

            print("Saving model to %s." % SAVE_PATH)
            sv.saver.save(session, SAVE_PATH, global_step=sv.global_step)


if __name__ == "__main__":
    main()
