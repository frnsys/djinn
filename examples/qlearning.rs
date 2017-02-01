extern crate rand;
extern crate djinn;

use rand::Rng;
use djinn::ext::qlearning::{QLearner, QLearnerParams};

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct State {
    x: usize,
    y: usize
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
enum Action {
    Left,
    Right,
    Up,
    Down
}

struct Environment {
    map: Vec<Vec<f64>>
}

struct Explorer {
    state: State,
    env: Environment,
    qlp: QLearnerParams<State, Action>,
}

impl Explorer {
    fn reset(&mut self) {
        self.state.x = self.qlp.rng.gen_range(0, self.env.map[0].len() - 1);
        self.state.y = self.qlp.rng.gen_range(0, self.env.map.len() - 1);
    }
}

impl QLearner for Explorer {
    type State = State;
    type Action = Action;

    fn reward(&self, state: &State) -> f64 {
        // -1 so each step is a penalty
        self.env.map[state.y][state.x] - 1.
    }

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
    let map = vec![
        vec![ 0., 0., 1.,-1.],
        vec![-1., 0.,-1., 0.],
        vec![ 0., 0.,-1., 0.],
        vec![ 0., 0., 0., 0.]
    ];

    let env = Environment {
        map: map
    };

    let mut explorer = Explorer {
        state: State {x: 0, y: 0},
        env: env,
        qlp: QLearnerParams::new(0.5, 0.5, 0.5)
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
            if r >= 0. {
                break;
            } else {
                match action {
                    Action::Up => explorer.state.y -= 1,
                    Action::Down => explorer.state.y += 1,
                    Action::Right => explorer.state.x += 1,
                    Action::Left => explorer.state.x -= 1
                }
            }
            i += 1;
        }
        println!("----DONE EPISODE: {:?}", reward);
        println!("Q: {:?}", explorer.qlp.q);
    }
}
