package app;

import java.time.LocalDate;
import java.time.Period;

public class Person {

    private String name;
    private String address;
    private String dateOfBirth;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(String dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public int getAge() {
        //Keeping date format "yyyy-MM-dd" for simplicity of age calculation
        LocalDate dob = LocalDate.parse(dateOfBirth);
        return Period.between(dob, LocalDate.now()).getYears();
    }
}
