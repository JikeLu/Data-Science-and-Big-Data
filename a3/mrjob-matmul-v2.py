from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

class MRMatrixMultiplyV2(MRJob):
    
    def configure_args(self):
        super(MRMatrixMultiplyV2, self).configure_args()
        self.add_passthru_arg('-M', '--matrix_sizes', help='Matrix sizes "m,n,p" where m*n is the size of matrix A and n*p is the size of matrix B')

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        sizes = list(map(int, self.options.matrix_sizes.split(',')))
        matrix, row, col, value = line.split()
        row, col, value = int(row), int(col), float(value)
        
        if matrix == 'A':
            for p in range(sizes[3]):
                yield (row, p), ('A', col, value)
        elif matrix == 'B':
            for m in range(sizes[0]):
                yield (m, col), ('B', row, value)

    def reducer(self, key, values):
        a_values = {}
        b_values = {}
        for value in values:
            if value[0] == 'A':
                a_values[int(value[1])] = float(value[2])
            elif value[0] == 'B':
                b_values[int(value[1])] = float(value[2])
        sum_result = sum(a_values[k] * b_values[k] for k in a_values if k in b_values)
        yield key, sum_result

if __name__ == '__main__':
    MRMatrixMultiplyV2.run()
