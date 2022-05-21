public class Main {
    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "--edu-level":
                EducationDriver eduDriver = new EducationDriver();
                eduDriver.run(args);
                break;
            case "--mnt-wine":
                WineDriver wineDriver = new WineDriver();
                wineDriver.run(args);
                break;
            case "--cli-cat":
                CategoryDriver categoryDriver = new CategoryDriver();
                categoryDriver.run(args);
                break;
            default:
                System.err.println("[!!] WARN: First argument needs to be one of (--edu-level, --mnt-wine, --cli-cat)");
                System.exit(1);
        }
    }
}
