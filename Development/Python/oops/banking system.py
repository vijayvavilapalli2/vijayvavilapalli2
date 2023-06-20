#Code written by Vijay
from abc import  ABCMeta,abstractmethod
from random import randint
class Account(metaclass=ABCMeta):
    @abstractmethod
    def authentication():
        return 0
    @abstractmethod
    def createAccount():
        return 0
    @abstractmethod
    def withDrawlAmmount():
        return 0
    @abstractmethod
    def deposite():
        return 0
    @abstractmethod
    def balanceEnquiery():
        return 0


class SavingsAccount(Account):
    def __init__(self):
        self.accountsDetails={}
    def authentication(self,name,accountNumber):
        if accountNumber in self.accountsDetails.keys():
            if self.accountsDetails[accountNumber][0]==name:
                print('Authentication is successfull!')
                return True
            else:
                print('Authentication failed!')
                return False
        else:
            print('Authentication failed!')
            return False
    def createAccount(self,name,initialDeposite):
        self.accountNumber=randint(11111,99999)
        self.accountsDetails[self.accountNumber]=[name,initialDeposite]
        print(f'Please note that you\'re account number is {self.accountNumber}')
        print('Account created successfully!')
        self.balanceEnquiery()
    def withDrawlAmmount(self,amount):
        if self.accountsDetails[self.accountNumber][1]<amount:
            print('Insufficient balance!')
            self.balanceEnquiery()
        else:
            self.accountsDetails[self.accountNumber][1]-=amount
            print('Withdrawl successful!!')
            self.balanceEnquiery()
    def deposite(self,depositeMoney):
        self.accountsDetails[self.accountNumber][1]+=depositeMoney
        print('Deposited successfully!')
        self.balanceEnquiery()
    def balanceEnquiery(self):
        print('Available balance:',self.accountsDetails[self.accountNumber][1])

objSavingsAccount=SavingsAccount()
while True:
    print('Enter 1 to create an account:')
    print('Enter 2 to login to the existing account:')
    print('Enter 3 to exit:')
    userChoice=int(input())
    if userChoice is 1:
        print('Please you\'re name:')
        name=input()
        print('Please enter initial deposit amount:')
        amount=int(input())
        objSavingsAccount.createAccount(name,amount)
    elif userChoice is 2:
        print('Please enter your name:')
        name=input()
        print('Please enter you\'re account number:')
        accountNumber=int(input())
        authenticationStatus=objSavingsAccount.authentication(name,accountNumber)
        if authenticationStatus is True:
            while True:
                print('Enter 1 to deposit:')
                print('Enter 2 to withdraw:')
                print('Enter 3 to balance enquiry:')
                print('Enter 4 to exit:')
                userChoice=int(input())
                if userChoice is 1:
                    print('Please enter the amount that you want to deposit:')
                    amount=int(input())
                    objSavingsAccount.deposite(amount)
                elif userChoice is 2:
                    print('Please enter the amount that you want to withdraw:')
                    amount=int(input())
                    objSavingsAccount.withDrawlAmmount(amount)
                elif userChoice is 3:
                    objSavingsAccount.balanceEnquiery()
                elif userChoice is 4:
                    break
    elif userChoice is 3:
        quit()
