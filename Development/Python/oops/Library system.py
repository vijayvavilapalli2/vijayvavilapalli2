#Implementation library system using oops.


#Abstraction and encapcilation
class library:
    def __init__(self,libraryBooks):
        self.Availblebooks = libraryBooks
    def displayAllAvailableBooks(self,):

        print()
        print('Available books are:')
        for book in self.Availblebooks:
            print(book)
    def lendBook(self,lendaBook):
        print()
        if lendaBook in self.Availblebooks:
            print(f'You can take this {lendaBook}!')
            self.Availblebooks.remove(lendaBook)
        else:
            print(f"The book {lendaBook} you're looking for is not availbale!")
    def addBook(self,submittedBook):
        print()
        self.Availblebooks.append(submittedBook)
        print(f'Thanks for returning the book of {submittedBook}')
class customer:
    def RequestForaBook(self):
        print('Please Enter the name of book you want:')
        self.book=input()
        return self.book
    def ReturnTheBook(self):
        print('Please Enter The name of book you want to return:')
        self.returnBook=input()
        return self.returnBook

librarySystem=library(['Who will cry whe you die','Atomic Habits','The Growth Mindset'])
librarySystem.displayAllAvailableBooks()
customerObj=customer()
while True:
    print('Want to see available books? -->Please select 1')
    print('Request for a book? -->Please select 2')
    print('Return a book?-->Please select 3')
    print('Exit ?-->Please select 4')
    userChoice=int(input('Please select an option:'))
    if userChoice is 1:
        librarySystem.displayAllAvailableBooks()
    elif userChoice is 2:
        RequestAbook=customerObj.RequestForaBook()
        librarySystem.lendBook(RequestAbook)
    elif userChoice is 3:
        ReturnAbook=customerObj.ReturnTheBook()
        librarySystem.addBook(ReturnAbook)

    elif userChoice is 4:
        quit()

