sentence = input("Enter a sentence : ")
words = sentence.split()
and_index = [i for i, word in enumerate(words) if word == 'and' ]

for i in and_index :
        words[i - 1: i + 2] = words[i + 1], 'and', words[i - 1]

interchanged = ' '.join(words)
print(interchanged)

