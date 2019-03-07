#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::ProbeHandle;

//use timely::progress::frontier::MutableAntichain;
use timely::dataflow::operators::unordered_input::UnorderedInput;

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

impl<D1, D2, T1, T2> InputChoice<D1, D2, T1, T2> {
    fn new_input1(d1: D1, t1: T1) -> InputChoice<D1, D2, T1, T2> {
        InputChoice::Input1(d1, t1, 1)
    }
    fn new_input2(d2: D2, t2: T2) -> InputChoice<D1, D2, T1, T2> {
        InputChoice::Input2(d2, t2, 1)
    }
}


fn main() {
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


        let mut input_queue = Vec::new();
        //let x :InputChoice<_, (i32, i32), _, isize> = InputChoice::Input1((0, 1), 0, 1);
        //let xx = create_input(1, (0, 1), (0, 1), 0, 0);

        //let yy :InputChoice<_, (i32, i32), _, isize> = InputChoice::new_input1((0,1), 0);
        //input_queue.push(InputChoice::Input1((0, 1), 0, 1));
        input_queue.push(InputChoice::new_input1((0, 1), 0));

        input_queue.push(InputChoice::Input1((3, 1), 3, 1));

        input_queue.push(InputChoice::Input1((2, 4), 1, 1));

        input_queue.push(InputChoice::Frontier1(4));


        input_queue.push(InputChoice::Input1((5, 2), 3, 1));
//
//        input_queue.push(InputChoice::new_input2((0, 8), 0));
//        input_queue.push(InputChoice::Input2((9, 1), 0, 1));
//        input_queue.push(InputChoice::Input2((9, 8), 1, 1));
//        input_queue.push(InputChoice::Frontier2(1));
//        input_queue.push(InputChoice::Input2((9, 4), 9, 1));
//        input_queue.push(InputChoice::Input2((3, 1), 9, 1));
//        input_queue.push(InputChoice::Frontier2(10));
//        input_queue.push(InputChoice::new_input1((4, 1), 0));

        // RENATO STARTS HERE
        // Written for general timestamps T1 and T2.
        {
            for input_choice in input_queue {

                println!("Processing: {:?}", input_choice);

                match input_choice {
                    InputChoice::Input1(data, time, diff) => {
//                        println!("edge_cap1={:?}", edge_cap1.time().vector);
                        let coord1 = if edge_cap1.time().vector.len() == 0 { Default::default() } else { edge_cap1.time().vector[0] };
                        let coord2 = if edge_cap2.time().vector.len() == 0 { Default::default() } else { edge_cap2.time().vector[1] };

                        if coord1 <= time {
                            let new_pair = Vector::new(vec![time, coord2]);
                            edge_input1
                                .session(edge_cap1.delayed(&new_pair))
                                .give((data, new_pair, diff));
                        } else {
                            println!("-LATE");
                        }
                    }
                    InputChoice::Input2(data, time, diff) => {
                        let coord1 = if edge_cap1.time().vector.len() == 0 { Default::default() } else { edge_cap1.time().vector[0] };
                        let coord2 = if edge_cap2.time().vector.len() == 0 { Default::default() } else { edge_cap2.time().vector[1] };

                        if coord2 <= time {
                            let new_pair = Vector::new(vec![coord1, time]);
                            edge_input2
                                .session(edge_cap2.delayed(&new_pair))
                                .give((data, new_pair, diff));
                        } else {
                            println!("-LATE");
                        }
                    }
                    InputChoice::Frontier1(time) => {
                        // Assuming changes coming Frontier1 still need to be simplified
                        let coord2 =  if edge_cap2.time().vector.len() == 0 { Default::default() } else {edge_cap2.time().vector[1]};
                        let pair_time = Vector::new(vec![time, coord2]);
                        edge_cap1.downgrade(&pair_time);
                        edge_cap2.downgrade(&pair_time);
                    }
                    InputChoice::Frontier2(time) => {
                        let coord1 =  if edge_cap1.time().vector.len() == 0 { Default::default() } else {edge_cap1.time().vector[0]};
                        let pair_time = Vector::new(vec![coord1, time]);
                        edge_cap1.downgrade(&pair_time);
                        edge_cap2.downgrade(&pair_time);
                    }
                };

//                edge_cap1.downgrade(&pair_time);
//                edge_cap2.downgrade(&pair_time);

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