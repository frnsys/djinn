//! # Q-Learning example
//! A simple demonstration of using Djinn's Q-learner.
//! Creates a Q-learning agent in a small grid world that
//! learns to find the fastest and safest path to a reward.

extern crate rand;
extern crate djinn;

use rand::Rng;
use djinn::ext::qlearning::{QLearner, QLearnerParams};

/// Explorer state is just its x, y position.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct State {
    x: usize,
    y: usize,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
enum Action {
    Left,
    Right,
    Up,
    Down,
}

struct Environment {
    map: Vec<Vec<f64>>,
}

struct Explorer {
    state: State,
    env: Environment,
    qlp: QLearnerParams<State, Action>,
}

impl Explorer {
    /// Reset the explorer's state.
    fn reset(&mut self) {
        self.state.x = self.qlp.rng.gen_range(0, self.env.map[0].len() - 1);
        self.state.y = self.qlp.rng.gen_range(0, self.env.map.len() - 1);
    }
}

impl QLearner for Explorer {
    type State = State;
    type Action = Action;

    /// Reward for a given state.
    fn reward(&self, state: &State) -> f64 {
        // -1 so each step is a penalty
        self.env.map[state.y][state.x] - 1.
    }

    /// Actions are limited by the bounds of the map;
    /// i.e. this is a non-toroidal space.
    /// So just check to see which moves are valid.
    fn actions(&self, state: &State) -> Vec<Action> {
        let mut actions = Vec::new();
        if state.x > 0 {
            actions.push(Action::Left);
        }
        if state.x < self.env.map[0].len() - 1 {
            actions.push(Action::Right);
        }
        if state.y > 0 {
            actions.push(Action::Up);
        }
        if state.y < self.env.map.len() - 1 {
            actions.push(Action::Down);
        }
        actions
    }

    fn params(&mut self) -> &mut QLearnerParams<State, Action> {
        &mut self.qlp
    }
}

fn main() {
    let map = vec![vec![0., 0., 1., -1.],
                   vec![-1., 0., -1., 0.],
                   vec![0., 0., -1., 0.],
                   vec![0., 0., 0., 0.]];

    let env = Environment { map: map };

    let mut explorer = Explorer {
        state: State { x: 0, y: 0 },
        env: env,
        qlp: QLearnerParams::new(0.5, 0.5, 0.5),
    };

    let n_episodes = 100;
    for _ in 0..n_episodes {
        explorer.reset();
        let mut i = 0;
        let mut reward = 0.;
        loop {
            println!("----STEP: {:?}", i);
            let s = explorer.state.clone();
            println!("state: {:?}", s);
            let r = explorer.reward(&s);
            println!("reward: {:?}", r);
            let action = explorer.choose_action(&s);
            println!("action: {:?}", action);
            reward += r;

            // episode is done when the explorer
            // finds the positive reward
            if r >= 0. {
                break;
            } else {
                // update state based on action;
                // i.e. move
                match action {
                    Action::Up => explorer.state.y -= 1,
                    Action::Down => explorer.state.y += 1,
                    Action::Right => explorer.state.x += 1,
                    Action::Left => explorer.state.x -= 1,
                }
            }
            i += 1;
        }
        println!("----DONE EPISODE: {:?}", reward);
        println!("Q: {:?}", explorer.qlp.q);
    }
}
