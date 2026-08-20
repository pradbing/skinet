using System;
using System.ComponentModel.DataAnnotations;

namespace API.DTOs;

public class RegisterDto
{
    [Required]
    public string firstName { get; set; } = string.Empty;
    [Required]
    public string lastName { get; set; } = string.Empty;
    [Required]
    public string email { get; set; } = string.Empty;
    [Required]
    public string password { get; set; } = string.Empty;
}
