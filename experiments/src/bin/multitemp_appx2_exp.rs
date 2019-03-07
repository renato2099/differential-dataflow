#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

extern crate math;
extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::ProbeHandle;
use math::round;

//use timely::progress::frontier::MutableAntichain;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use rand::{Rng, SeedableRng, StdRng};

use rand::distributions::{Uniform, Distribution};

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;

//use pair::Pair;
use vector::Vector;


// TODO T1 and T2 should implement Timestamp+Lattice
#[derive(Debug)]
pub enum InputChoice<D1, D2, T1, T2> {
    Input1(D1, T1, isize),
    Frontier1(T1),
    Input2(D2, T2, isize),
    Frontier2(T2),
}

//impl<D1, D2, T1, T2> InputChoice<D1, D2, T1, T2> {
//    fn new_input1(d1: D1, t1: T1) -> InputChoice<D1, D2, T1, T2> {
//        InputChoice::Input1(d1, t1, 1)
//    }
//    fn new_input2(d2: D2, t2: T2) -> InputChoice<D1, D2, T1, T2> {
//        InputChoice::Input2(d2, t2, 1)
//    }
//}


fn main() {
    // percentage of inserts, rest will be deletions
    let inserts_percent: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    // total number of elements
    let total_elems: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    // percentage of the total number of elements that belong to input1
    let i1_percent: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    // percentage of the total number of elements that belong to input1
    let num_frontiers: usize = std::env::args().nth(4).unwrap().parse().unwrap();

    let max_key = 100;
    let max_time = 100;
    let elems_per_frontier1 = round::ceil(total_elems as f64 * i1_percent as f64*0.01, 2) as usize / num_frontiers;
    let elems_per_frontier2 = (total_elems - elems_per_frontier1*num_frontiers) / num_frontiers;
    println!("elements per frontier1={}", elems_per_frontier1);
    println!("elements per frontier2={}", elems_per_frontier2);

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut probe = ProbeHandle::new();

        let (
            (mut root_input, root_cap),
            (mut edge_input1, mut edge_cap1),
            (mut edge_input2, mut edge_cap2),
        ) =
            worker.dataflow(|scope| {
                let ((root_input, root_cap), roots) = scope.new_unordered_input();
                let ((edge_input1, edge_cap1), edges1) = scope.new_unordered_input();
                let ((edge_input2, edge_cap2), edges2) = scope.new_unordered_input();

                let roots = roots.as_collection();
                let edges = edges1.as_collection().concat(&edges2.as_collection());

                edges.count()
                    .inspect(|x| println!("changes: {:?}", x))
                    .probe_with(&mut probe);

                ((root_input, root_cap), (edge_input1, edge_cap1), (edge_input2, edge_cap2))
            });

        // load initial root.
        root_input
            .session(root_cap)
            .give((0, Vector::new(vec![0, 0]), 1));

        let seed = [1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5];
        let seed2 = [1, 1, 1, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for choosing input stream
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for inserts/deletes
        let mut rng22: StdRng = SeedableRng::from_seed(seed);    // rng for inserts/deletes
        let mut rng3: StdRng = SeedableRng::from_seed(seed);    // rng for keys
        let mut rng4: StdRng = SeedableRng::from_seed(seed);    // rng for frontier1
        let mut rng5: StdRng = SeedableRng::from_seed(seed2);    // rng for frontier2

        let mut tot_input1 = elems_per_frontier1 * num_frontiers;
        let mut tot_input2 = total_elems - tot_input1;

        let frontiers1_range = Uniform::new_inclusive(1, max_time);
        let mut frontiers_dim1: Vec<i32> = frontiers1_range.sample_iter(&mut rng4).take(num_frontiers).collect();
        frontiers_dim1.push(0);
        frontiers_dim1.sort();

        let frontiers2_range = Uniform::new_inclusive(1, max_time);
        let mut frontiers_dim2: Vec<i32> = frontiers2_range.sample_iter(&mut rng5).take(num_frontiers).collect();
        frontiers_dim2.push(0);
        frontiers_dim2.sort();

        println!("{:?} -> {}", frontiers_dim1, tot_input1);
        println!("{:?} -> {}", frontiers_dim2, tot_input2);

        {
            let mut cnt_input1 = 0;
            let mut cnt_input2 = 0;
            let mut f1_low = 0;
            let mut f1_high = 1;
            let mut f2_low = 0;
            let mut f2_high = 1;
            for _ in 1..=total_elems {

                let input_prob = rng1.gen_range(0.0, 1.0);
                let data_key = rng3.gen_range(1, max_key);

                let mut input_choice = InputChoice::Input1(0, 0, 1);

                if tot_input2 == 0 || (tot_input1 > 0 && input_prob <= i1_percent as f64 *0.01) {
                    let ins_del = if rng2.gen_range(0.0, 1.0) <= inserts_percent as f64 *0.01 { 1 } else { -1 };
                    let time = rng4.gen_range(frontiers_dim1[f1_low], frontiers_dim1[f1_high]);
                    input_choice = InputChoice::Input1(data_key, time, ins_del);
                    println!("Processing: {:?}", input_choice);
                    {
                        let coord1 = if edge_cap1.time().vector.len() == 0 { Default::default() } else { edge_cap1.time().vector[0] };
                        let coord2 = if edge_cap2.time().vector.len() == 0 { Default::default() } else { edge_cap2.time().vector[1] };

                        if coord1 <= time {
                            let new_pair = Vector::new(vec![time, coord2]);
                            edge_input1
                                .session(edge_cap1.delayed(&new_pair))
                                .give((data_key, new_pair, ins_del));
                        } else {
                            println!("-LATE");
                        }
                    }
                    cnt_input1 += 1;
                    tot_input1 -= 1;
                    if cnt_input1 == elems_per_frontier1 {
                        println!("Change frontier_1_={}, {}", frontiers_dim1[f1_low], frontiers_dim1[f1_high]);
                        // emit frontier1
                        {
                            // Assuming changes coming Frontier1 still need to be simplified
                            let coord2 =  if edge_cap2.time().vector.len() == 0 { Default::default() } else {edge_cap2.time().vector[1]};
                            let pair_time = Vector::new(vec![frontiers_dim1[f1_low], coord2]);
                            edge_cap1.downgrade(&pair_time);
                            edge_cap2.downgrade(&pair_time);
                        }
                        f1_low = f1_high;
                        f1_high += 1;
                        cnt_input1 = 0;
                    }
                } else if tot_input2 > 0 || tot_input1 == 0 {
                    let ins_del = if rng22.gen_range(0.0, 1.0) <= inserts_percent as f64 *0.01 { 1 } else { -1 };
                    let time = rng4.gen_range(frontiers_dim2[f2_low], frontiers_dim2[f2_high]);
                    input_choice = InputChoice::Input2(data_key, time, ins_del);
                    println!("Processing: {:?}", input_choice);
                    {
                        let coord1 = if edge_cap1.time().vector.len() == 0 { Default::default() } else { edge_cap1.time().vector[0] };
                        let coord2 = if edge_cap2.time().vector.len() == 0 { Default::default() } else { edge_cap2.time().vector[1] };

                        if coord2 <= time {
                            let new_pair = Vector::new(vec![coord1, time]);
                            edge_input2
                                .session(edge_cap2.delayed(&new_pair))
                                .give((data_key, new_pair, ins_del));
                        } else {
                            println!("-LATE");
                        }
                    }
                    cnt_input2 += 1;
                    tot_input2 -= 1;
                    if cnt_input2 == elems_per_frontier2 {
                        println!("Change frontier_2_={}, {}", frontiers_dim2[f2_low], frontiers_dim2[f2_high]);
                        // emit frontier2
                        {
                            let coord1 =  if edge_cap1.time().vector.len() == 0 { Default::default() } else {edge_cap1.time().vector[0]};
                            let pair_time = Vector::new(vec![coord1, frontiers_dim2[f2_low]]);
                            edge_cap1.downgrade(&pair_time);
                            edge_cap2.downgrade(&pair_time);
                        }

                        f2_low = f2_high;
                        f2_high += 1;
                        cnt_input2 = 0;
                    }
                }

                // Run the computation until progress reported in output.
                while probe.less_than(edge_cap1.time()) {
                    worker.step();
                }

                probe.with_frontier(|f| {
                    print!("Done Frontier:");
                    for t in f.iter() {
                        println!("\t{:?}", t);
                    }
                });
            }
        }
        // RENATO ENDS HERE
    }).unwrap();
}

/// This module contains a definition of a new timestamp time, a "pair" or product.
///
/// This is a minimal self-contained implementation, in that it doesn't borrow anything
/// from the rest of the library other than the traits it needs to implement. With this
/// type and its implementations, you can use it as a timestamp type.
mod vector {
    /// A pair of timestamps, partially ordered by the product order.
    #[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Abomonation, Debug)]
    pub struct Vector<T> {
        pub vector: Vec<T>,
    }

    impl<T> Vector<T> {
        /// Create a new pair.
        pub fn new(vector: Vec<T>) -> Self {
            Vector { vector }
        }
    }

    // Implement timely dataflow's `PartialOrder` trait.
    use timely::order::PartialOrder;

    impl<T: PartialOrder + Default> PartialOrder for Vector<T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.vector
                .iter()
                .enumerate()
                .all(|(index, time)| time.less_equal(other.vector.get(index).unwrap_or(&Default::default())))
        }
    }

    use timely::progress::timestamp::Refines;

    impl<T: Timestamp> Refines<()> for Vector<T> {
        fn to_inner(_outer: ()) -> Self { Default::default() }
        fn to_outer(self) -> () { () }
        fn summarize(_summary: <Self>::Summary) -> () { () }
    }

    // Implement timely dataflow's `PathSummary` trait.
    // This is preparation for the `Timestamp` implementation below.
    use timely::progress::PathSummary;

    impl<T: Timestamp> PathSummary<Vector<T>> for () {
        fn results_in(&self, timestamp: &Vector<T>) -> Option<Vector<T>> {
            Some(timestamp.clone())
        }
        fn followed_by(&self, other: &Self) -> Option<Self> {
            Some(other.clone())
        }
    }

    // Implement timely dataflow's `Timestamp` trait.
    use timely::progress::Timestamp;

    impl<T: Timestamp> Timestamp for Vector<T> {
        type Summary = ();
    }

    // Implement differential dataflow's `Lattice` trait.
    // This extends the `PartialOrder` implementation with additional structure.
    use differential_dataflow::lattice::Lattice;

    impl<T: Lattice + Default + Clone> Lattice for Vector<T> {
        fn minimum() -> Self { Self { vector: Vec::new() } }
        fn join(&self, other: &Self) -> Self {
            let min_len = ::std::cmp::min(self.vector.len(), other.vector.len());
            let max_len = ::std::cmp::max(self.vector.len(), other.vector.len());
            let mut vector = Vec::with_capacity(max_len);
            for index in 0..min_len {
                vector.push(self.vector[index].join(&other.vector[index]));
            }
            for time in &self.vector[min_len..] {
                vector.push(time.clone());
            }
            for time in &other.vector[min_len..] {
                vector.push(time.clone());
            }
            Self { vector }
        }
        fn meet(&self, other: &Self) -> Self {
            let min_len = ::std::cmp::min(self.vector.len(), other.vector.len());
            let mut vector = Vec::with_capacity(min_len);
            for index in 0..min_len {
                vector.push(self.vector[index].meet(&other.vector[index]));
            }
            Self { vector }
        }
    }
}
