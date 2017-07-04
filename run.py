from app.app import SyncApp


def main():

    app = SyncApp(first_run=True)
    app.run()



if __name__ == "__main__":
    main()
