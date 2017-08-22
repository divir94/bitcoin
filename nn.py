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
                                    [config.batch_size, config.num_features])
        self.target = tf.placeholder(tf.float32,
                                     [config.batch_size, config.num_targets])

        inputs = self.input
        input_dimension = config.num_features
        for i, output_dimension in enumerate(config.layer_sizes):
            w = tf.get_variable("w" + str(i),
                                [input_dimension, output_dimension],
                                dtype=tf.float32)
            b = tf.get_variable("b" + str(i),
                                [output_dimension],
                                dtype=tf.float32)
            inputs = tf.nn.relu(tf.matmul(inputs, w) + b)
            input_dimension = output_dimension

        w_final = tf.get_variable("w_final",
                                  [input_dimension, config.num_targets],
                                  dtype=tf.float32)
        b_final = tf.get_variable("b_final",
                                  [config.num_targets],
                                  dtype=tf.float32)
        self.predictions = tf.matmul(inputs, w_final) + b_final
        self.loss = tf.reduce_mean(tf.abs(self.predictions - self.target))

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
    def __init__(self, path, batch_size):
        self.batch_size = batch_size
        with open(path, "rb") as f:
            xes, yes = pickle.load(f)
        self.length = len(xes)
        assert self.length == len(yes)
        self.pairs = zip(xes, yes)
        self._position = 0
        self.reset()

    def generate_next_batch(self):
        if self.length < self._position + self.batch_size:
            self.reset()
        batch = self.pairs[self._position: self._position + self.batch_size]
        batch_xes = [x for x, y in batch]
        batch_yes = [y for x, y in batch]
        return batch_xes, batch_yes

    def reset(self):
        self._position = 0
        random.shuffle(self.pairs)


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
