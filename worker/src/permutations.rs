pub fn permutations<T>(alphabet: &[T], max_length: usize) -> Permutations<T> 
    where T: Clone,
{
    Permutations::new(alphabet, max_length)
}

pub struct Permutations<T> {
    alphabet: Vec<T>,
    max_length: usize,
    total_count: usize,
    curr_index: usize,
}

impl<T> Permutations<T> 
    where T: Clone,
{
    fn new(alphabet: &[T], max_length: usize) -> Self {
        let alphabet_size = alphabet.len();
        let total_count = alphabet_size * (alphabet_size.pow(max_length as u32) - 1) / (alphabet_size - 1);
        let alphabet = alphabet.iter().cloned().collect();

        Self {
            max_length,
            total_count,
            alphabet,
            curr_index: 0,
        }
    }

    fn index_to_sequence(&self, index: usize) -> Vec<T> {
        let alphabet_size = self.alphabet.len();
        let mut idx = index;
        let mut length = 1;
        
        while length <= self.max_length {
            let count_at_length = alphabet_size.pow(length as u32);
            
            if idx < count_at_length {
                return self.number_to_sequence(idx, length);
            }
            
            idx -= count_at_length;
            length += 1;
        }
        
        panic!("Index out of bounds");
    }
    
    fn number_to_sequence(&self, mut num: usize, length: usize) -> Vec<T> {
        let alphabet_size = self.alphabet.len();
        let mut result = Vec::with_capacity(length);
        
        for _ in 0..length {
            let digit = (num % alphabet_size) as usize;
            result.push(self.alphabet[digit].clone());
            num /= alphabet_size;
        }
        
        result.reverse();
        result
    }
}

impl<T> Iterator for Permutations<T> 
    where T: Clone,
{
    type Item = Vec<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_index >= self.total_count {
            return None;
        }
        
        let result = self.index_to_sequence(self.curr_index);
        self.curr_index += 1;
        Some(result)
    }
    
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.curr_index += n;
        self.next()
    }
}
