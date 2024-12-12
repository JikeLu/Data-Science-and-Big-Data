from mrjob.job import MRJob
from mrjob.step import MRStep

class MRJumbleSolver(MRJob):

    def mapper(self, _, line):
        if not line.strip():
            return
        
        parts = line.strip().split()
        if len(parts) < 1:
            return
        
        word = parts[0]
        tag = parts[1] if len(parts) > 1 and parts[1] == '?' else ''
        sorted_word = ''.join(sorted(word))
        yield sorted_word, (word, tag)

    def reducer(self, sorted_word, word_tags):
        jumbled_words = []
        dictionary_words = []
        
        for word, tag in word_tags:
            if tag == '?':
                jumbled_words.append(word)
            else:
                dictionary_words.append(word)
        
        for jumbled_word in jumbled_words:
            if dictionary_words:
                result = [f"?{jumbled_word}"] + dictionary_words
                yield len(result), result

if __name__ == '__main__':
    MRJumbleSolver.run()
