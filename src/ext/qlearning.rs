use rand;
use rand::{Rng, ThreadRng};
use std::fmt::Debug;
use std::hash::Hash;
use std::collections::HashMap;

pub trait State: Send + Sync + Clone + PartialEq + Hash + Eq + Debug {}
impl<T> State for T where T: Send + Sync + Clone + PartialEq + Hash + Eq + Debug {}

pub trait Action: Send + Sync + Clone + PartialEq + Hash + Eq + Debug {}
impl<T> Action for T where T: Send + Sync + Clone + PartialEq + Hash + Eq + Debug {}

pub struct QLearnerParams<S: State, A: Action> {
    discount: f64,
    explore: f64,
    learning_rate: f64,
    prev: Option<(S, A)>,
    pub q: HashMap<S, HashMap<A, f64>>,
    pub rng: ThreadRng,
}

impl<S: State, A: Action> QLearnerParams<S, A> {
    pub fn new(discount: f64, explore: f64, learning_rate: f64) -> QLearnerParams<S, A> {
        QLearnerParams {
            discount: discount,
            explore: explore,
            learning_rate: learning_rate,
            prev: None,
            q: HashMap::new(),
            rng: rand::thread_rng(),
        }
    }
}

pub trait QLearner {
    type State: State;
    type Action: Action;

    fn reward(&self, state: &Self::State) -> f64;
    fn actions(&self, state: &Self::State) -> Vec<Self::Action>;
    fn params(&mut self) -> &mut QLearnerParams<Self::State, Self::Action>;

    /// Choose the best or a random action.
    fn choose_action(&mut self, state: &Self::State) -> Self::Action {
        let reward = self.reward(state);
        let actions = self.actions(state);
        let mut params = self.params();
        let action = {
            if params.rng.gen::<f64>() < params.explore {
                rand::sample(&mut params.rng, actions.clone(), 1)[0].clone()
            } else {
                if !params.q.contains_key(state) {
                    // initialize actions_rewards
                    let mut actions_rewards = HashMap::new();
                    for action in &actions {
                        actions_rewards.insert(action.clone(), 0.0);
                    }
                    params.q.insert(state.clone(), actions_rewards);

                    // dont know anything yet, choose random
                    rand::sample(&mut params.rng, actions.clone(), 1)[0].clone()
                } else {
                    let actions_rewards = params.q.get(state).unwrap();
                    let (action, _) = actions_rewards.iter()
                        .max_by(|&item1, &item2| {
                            let (_, val1) = item1;
                            let (_, val2) = item2;
                            val1.partial_cmp(val2).unwrap()
                        })
                        .unwrap();
                    action.clone()
                }
            }
        };

        learn(params, state, reward);
        params.prev = Some((state.clone(), action.clone()));
        action
    }
}

/// Update Q-value for last taken action.
fn learn<S: State, A: Action>(params: &mut QLearnerParams<S, A>, state: &S, reward: f64) -> () {
     match params.prev {
         Some(ref prev) => {
             let &(ref p_state, ref p_action) = prev;
             let val = {
                 let actions_values =
                     params.q.entry(p_state.clone()).or_insert_with(HashMap::new);
                 *actions_values.entry(p_action.clone()).or_insert(0.)
             };
             let best_next_val = match params.q.get(state) {
                 Some(ars) => {
                     // ugghhh
                     *ars.values().max_by(|&f1, &f2| f1.partial_cmp(f2).unwrap()).unwrap()
                 }
                 None => 0.0,
             };
             let mut actions_values = params.q.get_mut(&p_state).unwrap();
             let new_val = params.learning_rate *
                           (reward + params.discount * best_next_val) -
                           val;
             actions_values.insert(p_action.clone(), new_val);
         }
         None => (),
     }
}
