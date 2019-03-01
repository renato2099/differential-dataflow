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

use pair::Pair;

#[derive(Debug)]
pub enum InputChoice<D1,D2,T1,T2> {
    Input1(D1, T1, isize),
    SysFrontier(T1),
    Input2(D2, T2, isize),
    Frontier1(T2),
    Frontier2(T2),
}

impl<D1,D2,T1 ,T2> InputChoice<D1,D2,T1 ,T2> {
    fn new_input1 (d1: D1, t1:T1) -> InputChoice<D1, D2, T1, T2> {
        InputChoice::Input1(d1, t1, 1) 
    }
    fn new_input2 (d2: D2, t2:T2) -> InputChoice<D1, D2, T1, T2> {
        InputChoice::Input2(d2, t2, 1) 
    }
}

//fn new_default<T3, T1, T2>() -> Pair<T3, (T1, T2)>
fn new_default<T1, T2>() -> Pair<T1, T2>
where T1: std::default::Default, T2: std::default::Default
{
    let c1 = <T1 as Default >::default() ;
    let c2 = <T2 as Default>::default();
    Pair::new(c1, c2)
}

//fn create_input<D1,D2,T1,T2> (choice :isize, d1: D1, d2 :D2, t1:T1, t2:T2) -> InputChoice<D1, D2, T1, T2> {
//    match choice {
//        1 => { InputChoice::Input1(d1, t1, 1) },
//        2 => { InputChoice::Input2(d2, t2, 1) },
//        _ => { unimplemented!() }
//    }
//}

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let mut probe = ProbeHandle::new();

        let (
                (mut sys_input, mut sys_cap),
                (mut root_input, root_cap),
                (mut edge_input1, mut edge_cap1),
                (mut edge_input2, mut edge_cap2),
            ) =
        worker.dataflow(|scope| {

            let ((root_input, root_cap), _roots) = scope.new_unordered_input();
            let ((edge_input1, edge_cap1), edges1) = scope.new_unordered_input();
            let ((edge_input2, edge_cap2), edges2) = scope.new_unordered_input();
            let ((syst_input, syst_cap), _syst) = scope.new_unordered_input();

            let _roots = _roots.as_collection();
            let _syst = _syst.as_collection();
            let edges = edges1.as_collection().concat(&edges2.as_collection());

            edges.count()
                 .inspect(|x| println!("changes: {:?}", x))
                 .probe_with(&mut probe);

            ((syst_input, syst_cap), (root_input, root_cap), (edge_input1, edge_cap1), (edge_input2, edge_cap2))
        });

        // load initial root.
        root_input
            .session(root_cap)
            .give((0, Pair::new(0, 0), 1));

        let syspair = Pair::new(0, 0);
        sys_input
            .session(sys_cap.delayed(&syspair))
            .give((0, syspair, 1));


        let mut input_queue = Vec::new();
        //let x :InputChoice<_, (i32, i32), _, isize> = InputChoice::Input1((0, 1), 0, 1);
        //let xx = create_input(1, (0, 1), (0, 1), 0, 0);

        //let yy :InputChoice<_, (i32, i32), _, isize> = InputChoice::new_input1((0,1), 0);
        //input_queue.push(InputChoice::Input1((0, 1), 0, 1));
        input_queue.push(InputChoice::new_input1((0, 1), 0));

        input_queue.push(InputChoice::Input1((3, 1), 3, 1));

        //input_queue.push(InputChoice::Input1((2, 4), 1, 1));

        input_queue.push(InputChoice::SysFrontier(4));
        input_queue.push(InputChoice::Frontier1(3));

        //input_queue.push(InputChoice::Input1((5, 2), 3, 1));
        //input_queue.push(InputChoice::new_input2((0, 8), 0));
        input_queue.push(InputChoice::Input2((9, 1), 0, 1));
        input_queue.push(InputChoice::Input2((9, 8), 2, 1));
        input_queue.push(InputChoice::Input2((9, 8), 3, 1));
        //input_queue.push(InputChoice::Frontier2(1));
        //input_queue.push(InputChoice::Input2((9, 4), 9, 1));
        //input_queue.push(InputChoice::Input2((3, 1), 9, 1));
        input_queue.push(InputChoice::Frontier2(10));
        //input_queue.push(InputChoice::new_input1((4, 1), 0));

        // RENATO STARTS HERE
        // Written for general timestamps T1 and T2.
        {

            let mut oldest_t1 = 0;
            let mut oldest_t2 = 0;
            for input_choice in input_queue {

                println!("Processing: {:?}", input_choice);

                match input_choice {
                    InputChoice::Input1(data, time, diff) => {
                        oldest_t1 = if oldest_t1 > time {time} else {oldest_t1};
                        let ptime = Pair::new(sys_cap.time().first, time);
                        edge_input1
                            .session(edge_cap1.delayed(&ptime))
                            .give((data, ptime, diff));
                    },
                    InputChoice::Input2(data, time, diff) => {
                        oldest_t2 = if oldest_t2 > time {time} else {oldest_t2};
                        let ptime = Pair::new(sys_cap.time().first, time);
                        edge_input2
                            .session(edge_cap2.delayed(&ptime))
                            .give((data, ptime, diff));
                    },
                    InputChoice::SysFrontier(time) => {
                        let oldest = if oldest_t1 > oldest_t2 { oldest_t2 } else { oldest_t1 };
                        let pair_time = Pair::new(time, oldest);
                        sys_cap.downgrade(&pair_time);
                        edge_cap1.downgrade(&pair_time);
                        edge_cap2.downgrade(&pair_time);
                    },
                    InputChoice::Frontier1(time) => {
                        oldest_t1 = time;
                        let oldest = if time > oldest_t2 { oldest_t2 } else { time };
                        let pair_time = Pair::new(sys_cap.time().first, oldest);
                        sys_cap.downgrade(&pair_time);
                        edge_cap1.downgrade(&pair_time);
                        edge_cap2.downgrade(&pair_time);
                    },
                    InputChoice::Frontier2(time) => {
                        oldest_t2 = time;
                        let oldest = if time > oldest_t1 { oldest_t1 } else { time };
                        let pair_time = Pair::new(sys_cap.time().first, oldest);
                        sys_cap.downgrade(&pair_time);
                        edge_cap1.downgrade(&pair_time);
                        edge_cap2.downgrade(&pair_time);
                    },
                };


                // Run the computation until progress reported in output.
                //while probe.less_than(&pair_time) {
                while probe.less_than(sys_cap.time()) {
                //while probe.less_than(edge_cap1.time())  {
                    worker.step();
                }

                probe.with_frontier(|f| {
                    println!("Done Frontier:");
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
mod pair {

    /// A pair of timestamps, partially ordered by the product order.
    #[derive(Hash, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Abomonation)]
    pub struct Pair<S, T> {
        pub first: S,
        pub second: T,
    }

    impl<S, T> Pair<S, T> {
        /// Create a new pair.
        pub fn new(first: S, second: T) -> Self {
            Pair { first, second }
        }
    }

    // Implement timely dataflow's `PartialOrder` trait.
    use timely::order::PartialOrder;
    impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.first.less_equal(&other.first) && self.second.less_equal(&other.second)
        }
    }

    use timely::progress::timestamp::Refines;
    impl<S: Timestamp, T: Timestamp> Refines<()> for Pair<S, T> {
        fn to_inner(_outer: ()) -> Self { Default::default() }
        fn to_outer(self) -> () { () }
        fn summarize(_summary: <Self>::Summary) -> () { () }
    }

    // Implement timely dataflow's `PathSummary` trait.
    // This is preparation for the `Timestamp` implementation below.
    use timely::progress::PathSummary;

    impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S,T>> for () {
        fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S,T>> {
            Some(timestamp.clone())
        }
        fn followed_by(&self, other: &Self) -> Option<Self> {
            Some(other.clone())
        }
    }

    // Implement timely dataflow's `Timestamp` trait.
    use timely::progress::Timestamp;
    impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
        type Summary = ();
    }

    // Implement differential dataflow's `Lattice` trait.
    // This extends the `PartialOrder` implementation with additional structure.
    use differential_dataflow::lattice::Lattice;
    impl<S: Lattice, T: Lattice> Lattice for Pair<S, T> {
        fn minimum() -> Self { Pair { first: S::minimum(), second: T::minimum() }}
        fn maximum() -> Self { Pair { first: S::maximum(), second: T::maximum() }}
        fn join(&self, other: &Self) -> Self {
            Pair {
                first: self.first.join(&other.first),
                second: self.second.join(&other.second),
            }
        }
        fn meet(&self, other: &Self) -> Self {
            Pair {
                first: self.first.meet(&other.first),
                second: self.second.meet(&other.second),
            }
        }
    }

    use std::fmt::{Formatter, Error, Debug};

    /// Debug implementation to avoid seeing fully qualified path names.
    impl<TOuter: Debug, TInner: Debug> Debug for Pair<TOuter, TInner> {
        fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
            f.write_str(&format!("({:?}, {:?})", self.first, self.second))
        }
    }

}
