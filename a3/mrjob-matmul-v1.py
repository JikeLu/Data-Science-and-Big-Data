from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMatrixMultiplyLetters(MRJob):

    def configure_args(self):
        super(MRMatrixMultiplyLetters, self).configure_args()
        self.add_passthru_arg('-M', '--matrix-dimensions', help='Matrix dimensions as m,n,p (m*n and n*p)')

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        dimensions = list(map(int, self.options.matrix_dimensions.split(',')))
        m, n, p = dimensions[0], dimensions[1], dimensions[2]

        matrix, row, col, letter = line.split()
        row, col = int(row), int(col)
        
        if matrix == 'A':
            for i in range(p):
                yield (row, i), ('A', col, letter)
        elif matrix == 'B':
            for i in range(m):
                yield (i, col), ('B', row, letter)

    def reducer(self, key, values):
        a_letters = {}
        b_letters = {}
        for matrix, idx, letter in values:
            if matrix == 'A':
                if idx not in a_letters:
                    a_letters[idx] = []
                a_letters[idx].append(letter)
            else:
                if idx not in b_letters:
                    b_letters[idx] = []
                b_letters[idx].append(letter)
        
        result = []
        for k in a_letters:
            if k in b_letters:
                for letter_a in a_letters[k]:
                    for letter_b in b_letters[k]:
                        result.append(letter_a + letter_b)
        
        yield key, result

if __name__ == '__main__':
    MRMatrixMultiplyLetters.run()
